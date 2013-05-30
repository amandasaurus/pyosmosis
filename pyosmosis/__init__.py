from xml.etree import ElementTree
import bz2, time
from contextlib import contextmanager
import argparse, inspect

version = (0, 0, 1)

def version_header():
    return "pyosmosis {0}.{1}.{2}".format(version)

class OSMElement(object):
    def to_xml(self, indent=0):
        tag_name = self.__class__.__name__.lower()
        lines = [u"<{0}".format(tag_name)+((" "+u" ".join(u'{key}="{value}"'.format(key=key, value=value) for key, value in self.attrs.items())) if len(self.attrs) > 0 else "")+">"]
        for key, value in self.tags.items():
            # FIXME proper xml escapeing
            lines.append(u' <tag k="{k}" v="{v}" />'.format(k=key, v=value))

        lines.extend(self.to_xml_extras(indent=(indent+2)))

        lines.append(u"</{0}>".format(tag_name))

        if indent > 0:
            lines = [" "*indent+line for line in lines]

        lines = [line.encode("utf8") for line in lines]

        return "\n".join(lines)

    def to_xml_extras(self, indent=0):
        return []

class Node(OSMElement):
    pass

class Way(OSMElement):
    def __init__(self, *args, **kwargs):
        super(Way, self).__init__(*args, **kwargs)
        self.node_ids = []

    def to_xml_extras(self, indent=0):
        # FIXME proper XML escaping
        lines = ['<nd ref="{0}" />'.format(node_id) for node_id in self.node_ids]
        if indent > 0:
            lines = [" "*indent+line for line in lines]
        return lines


class Relation(OSMElement):
    def __init__(self, *args, **kwargs):
        super(Relation, self).__init__(*args, **kwargs)
        self.members = []

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
            # FIXME generator line & details

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
def log():
    # FIXME allow options in cmd line

    def inner(input_stream):
        num_elements = 0
        time_of_last_msg = 0
        log_every = 10
        for el in input_stream:
            yield el

            num_elements += 1

            now = time.time()
            if (now - time_of_last_msg) >= log_every:
                print "Processed %d elements" % num_elements
                time_of_last_msg = now
            

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

