class AttributeMapper(object):
    """Map source attributes to target attributes."""

    def __init__(self, attribute_map=None):
        """Can pass attribute_map dict."""

        self.attribute_map = attribute_map if isinstance(attribute_map, dict) else {}

    def add_mapping(self, src_attr, tgt_attr):
        """Add single attribute name mapping. Overwrites existing mapping."""

        self.attribute_map[src_attr] = tgt_attr

    def remove_mapping(self, src_attr):
        """Remove single attribute name mapping."""

        self.attribute_map.pop(src_attr)
