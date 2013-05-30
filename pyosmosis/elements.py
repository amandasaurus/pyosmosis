

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

