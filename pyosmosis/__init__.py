from __future__ import division

from xml.etree import ElementTree
import bz2, time
from contextlib import contextmanager
import argparse, inspect

from elements import Node, Way, Relation, OSMElement

version = (0, 0, 1)

def version_header():
    return "pyosmosis {0}.{1}.{2}".format(*version)

@contextmanager
def open_optional_compression(filename, mode="r"):
    if filename.lower().endswith(".bz2"):
        with bz2.BZ2File(filename, mode) as fp:
            yield fp
    else:
        with open(filename, mode) as fp:
            yield fp



pipeline_elements = []

def pipeline_element(func):
    global pipeline_elements
    pipeline_elements.append(func)
    return func

def convert_values_to_args_kwargs(function, values):
    if all('=' in v for v in values):
        args = []
        kwargs = dict(v.split("=", ) for v in values)
    elif not all('=' in v for v in values):
        args = values
        kwargs = {}
    else:
        raise ValueError

    return args, kwargs

def make_action(elem):
    class Action(argparse.Action):
        def __call__(self, parser, namespace, values, option_string=None):
            if not 'pipeline' in namespace:
                namespace.pipeline = []

            args, kwargs = convert_values_to_args_kwargs(elem, values)
            generator = elem(*args, **kwargs)
            # FIXME confirm it's a generator
            namespace.pipeline.append(generator)

    return Action

def add_elements_to_arg_parser(parser):
    global pipeline_elements
    for elem in pipeline_elements:


        name = elem.__name__.replace("_", "-")
        parser.add_argument("--"+name, nargs='*', action=make_action(elem))

    return parser
    


@pipeline_element
def read_xml(filename):
    def inner():
        current_object = None
        current_tags = None
        with open_optional_compression(filename) as fp:
            for event, elem in ElementTree.iterparse(fp, events=['start', 'end']):
                if elem.tag in ['osm', 'bounds']:
                    # ignore
                    continue
                elif elem.tag == 'node':
                    if event == 'start':
                        current_object = Node()
                        current_object.attrs = elem.attrib
                        current_object.tags = {}
                    elif event == 'end':
                        yield current_object
                elif elem.tag == 'tag':
                    if event == 'start':
                        key, value = elem.attrib['k'], elem.attrib['v']
                        current_object.tags[key] = value
                elif elem.tag == 'way':
                    if event == 'start':
                        current_object = Way()
                        current_object.attrs = elem.attrib
                        current_object.tags = {}
                    elif event == 'end':
                        yield current_object
                elif elem.tag == 'relation':
                    if event == 'start':
                        current_object = Relation()
                        current_object.attrs = elem.attrib
                        current_object.tags = {}
                    elif event == 'end':
                        yield current_object
                elif elem.tag == 'nd':
                    if event == 'start':
                        current_object.node_ids.append(elem.attrib['ref'])
                elif elem.tag == 'member':
                    if event == 'start':
                        current_object.members.append(elem.attrib)
                else:
                    import pdb ; pdb.set_trace()
                    
    return inner


@pipeline_element
def write_xml(filename):
    def inner(input_stream):
        with open_optional_compression(filename, "w") as fp:
            # FIXME proper streaming XML writer
            fp.write('<?xml version="1.0" encoding="UTF-8"?>\n')
            fp.write('<osm version="0.6" generator="{0}">\n'.format(version_header()))

            for element in input_stream:
                if isinstance(element, OSMElement):
                    fp.write(element.to_xml(indent=2))
                    fp.write("\n")
                else:
                    raise TypeError

            fp.write("</osm>")

        # We have to include one yield so it'll know it's a generator
        yield None
        
        
    return inner

def whitelist(func):
    def inner(input_stream):
        for element in input_stream:
            if func(element):
                yield element
    return inner

def blacklist(func):
    def inner(input_stream):
        for element in input_stream:
            if not func(element):
                yield element
    return inner


@pipeline_element
def only_nodes():
    return whitelist(lambda element: isinstance(element, Node))

@pipeline_element
def reject_nodes():
    return blacklist(lambda element: isinstance(element, Node))
    
@pipeline_element
def only_ways():
    return whitelist(lambda element: isinstance(element, Way))

@pipeline_element
def reject_ways():
    return blacklist(lambda element: isinstance(element, Way))
    
@pipeline_element
def only_relations():
    return whitelist(lambda element: isinstance(element, Relation))

@pipeline_element
def reject_relations():
    return blacklist(lambda element: isinstance(element, Relation))
    

@pipeline_element
def sort():
    def inner(input_stream):
        nodes, ways, relations = {}, {}, {}
        for element in input_stream:
            if isinstance(element, Node):
                nodes[element.attrs['id']] = element
            elif isinstance(element, Way):
                ways[element.attrs['id']] = element
            elif isinstance(element, Relation):
                relations[element.attrs['id']] = element
            else:
                raise TypeError

        for node_id in sorted(nodes.keys()):
            yield nodes[node_id]
        for way_id in sorted(ways.keys()):
            yield ways[way_id]
        for relation_id in sorted(relations.keys()):
            yield relations[relation_id]

    return inner

@pipeline_element
def write_null():
    def inner(input_stream):
        for el in input_stream:
            pass

    return inner


@pipeline_element
def log(log_every=10, prefix=None):
    log_every = float(log_every)
    prefix = prefix or ""

    def inner(input_stream):
        num_elements = 0
        time_of_last_msg = 0
        for el in input_stream:
            yield el

            num_elements += 1

            now = time.time()
            if (now - time_of_last_msg) >= log_every:
                print "%sProcessed %d elements" % (prefix, num_elements)
                time_of_last_msg = now
            
    return inner


@pipeline_element
def default_tags(**tags):
    def inner(input_stream):
        for el in input_stream:
            for key, value in tags.items():
                el.tags[key] = el.tags[key] if key in el.tags else value
            yield el

    return inner

@pipeline_element
def limit(num=0):
    num = int(num)
    def inner(input_stream):
        num_so_far = 0
        for el in input_stream:
            if num > 0:
                if num_so_far < num:
                    # below limit
                    yield el
                    num_so_far += 1
                else:
                    # Reached the max
                    break
            else:
                # No limit, so loop all the way
                yield el

    return inner
        

@pipeline_element
def bbox(top, left, bottom, right):
    top, left, bottom, right = float(top), float(left), float(bottom), float(right)
    assert bottom <= top, (top, bottom)
    assert left <= right, (left, right)
    assert -90 <= top <= 90
    assert -90 <= bottom <= 90
    assert -180 <= left <= 180
    assert -180 <= right <= 180

    def inner(input_stream):
        nodes_that_passed = set()
        ways_that_passed = set()
        relations_that_passed = set()
        for el in input_stream:
            if isinstance(el, Node):
                lat = float(el.attrs['lat'])
                lon = float(el.attrs['lon'])
                if bottom <= lat <= top and left <= lon <= right:
                    nodes_that_passed.add(el.attrs['id'])
                    yield el
            elif isinstance(el, Way):
                if any(node_id in nodes_that_passed for node_id in el.node_ids):
                    ways_that_passed.add(el.attrs['id'])
                    yield el
            elif isinstance(el, Relation):
                should_pass = False
                for member in el.members:
                    if member['type'] == 'node':
                        if member['ref'] in nodes_that_passed:
                            should_pass = True
                            break
                    elif member['type'] == 'way':
                        if member['ref'] in ways_that_passed:
                            should_pass = True
                            break
                    elif member['type'] == 'relation':
                        if member['ref'] in relations_that_passed:
                            should_pass = True
                            break
                    else:
                        raise ValueError("Unknown member type", member)
                if should_pass:
                    relations_that_passed.add(el.attrs['id'])
                    yield el
            else:
                raise ValueError("Unknown element", el)

    return inner

@pipeline_element
def used_nodes():
    """
    Only returns nodes that are used in a way or relation.
    NB: will re-order stream, nodes will come last
    """
    def inner(input_stream):
        nodes = {}
        used_nodes = set()
        for el in input_stream:
            if isinstance(el, Node):
                nodes[el.attrs['id']] = el
            elif isinstance(el, Way):
                used_nodes.update(el.node_ids)
                yield el
            elif isinstance(el, Relation):
                used_nodes.update([x['ref'] for x in el.members if x['type'] == 'node'])
                yield el
            else:
                raise ValueError("Unknown element", el)

        # Now spit out the nodes 
        for node_id in used_nodes:
            if node_id in nodes:
                yield nodes[node_id]
            else:
                # log error?
                pass


    return inner


class Pipeline(object):
    def __init__(self, *elements):
        assert len(elements) > 1
        pipeline = elements[0]()
        for el in elements[1:]:
            pipeline = el(pipeline)
        self.pipeline = pipeline

    def run(self):
        # iterate over that
        list(self.pipeline)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    add_elements_to_arg_parser(parser)

    args = parser.parse_args()
    if not hasattr(args, 'pipeline') or len(args.pipeline) == 0:
        parser.print_help()
    else:
        pipeline = Pipeline(*args.pipeline)
        pipeline.run()

