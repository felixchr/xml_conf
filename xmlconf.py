# coding: utf-8

import re
import logging
from collections import OrderedDict
from copy import copy

logger = logging.getLogger(__name__)

try:
    import xml.etree.cElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET

from lxml import etree

def uniq_seq_merge(seq1, seq2):
    new_seq = copy(seq1)
    items_only_in_seq2 = set(seq2) - set(new_seq)
    len2 = len(seq2)
    for item in items_only_in_seq2:
        i2 = seq2.index(item)
        if i2 == len2 - 1:
            new_seq.append(item)
        else:
            for i in range(i2 + 1, len2):
                key = seq2[i]
                if key in new_seq:
                    new_seq.insert(new_seq.index(key), item)
                    break
            else:
                new_seq.append(item)
    return new_seq

def xpath_add_default_ns(xpath, def_ns='__default__'):
    '''
        Add a default ns tag. This is because lxml doesn't support empty namespace
    '''
    def find_brackets(xpath):
        ret = []
        stack = []
        p = re.compile(r'([\[\]])')
        for e in p.finditer(xpath):
            string = e.group()
            index = e.start()
            if string == '[':
                stack.append(index)
            else:
                ret.append((stack[-1], index))
                stack.pop(-1)
        return ret
    def in_brackets(index, brackets):
        if not brackets:
            return False
        for start, end in brackets:
            if start < index < end:
                return True
        return False

    ret = []
    p = re.compile(r'/([^\[\]/\'\"]+)')
    last_end = 0
    brackets = find_brackets(xpath)
    for match in p.finditer(xpath):
        string = match.group()
        start = match.start()
        end = match.end()
        if in_brackets(start, brackets):
            ret.append(xpath[last_end:end])
            last_end = end
        else:
            # is a real node tag
            if ':' in string:
                # has a name space in name
                ret.append(xpath[last_end:end])
                last_end = end
            else:
                ret.append(xpath[last_end:start])
                ret.append('/' + def_ns + ':' + string[1:])
                last_end = end
    if end < len(xpath) - 1:
        ret.append(xpath[last_end:])
    return ''.join(ret)

class XmlConfig:
    '''
        using lxml to process xml format file
    '''
    def __init__(self, filename: str='', lines: list=None) -> None:
        if filename:
            et = etree.parse(filename)
        elif not lines:
            et = etree.fromstringlist(lines)
        self.filename = filename
        self.etree = et
        self.root = et.getroot()
        self._nsmap_r = dict([(value, key) for key, value in self.root.nsmap.items()])
        self._children_count = {}
        self._dict = OrderedDict()
        self._node_map = {}
        self._get_node_list()

    def search(self, xpath, name_def_ns='__default__'):
        '''search node which matches xpath'''
        if self._nsmap_r:
            # the file is with namespaces defined
            nsmap = copy(self.root.nsmap)
            if None in nsmap:
                nsmap[name_def_ns] = nsmap[None]
                nsmap.pop(None)
                xpath = xpath_add_default_ns(xpath, name_def_ns)
            return self.etree.xpath(xpath, namespaces=nsmap)
        return self.etree.xpath(xpath)

    def _rm_prefix(self, tag: str):
        # remove the namespace in tag
        if '}' in tag:
            ns, _, name = tag.partition('}')
            ns = ns[1:]
            if self._nsmap_r[ns]:
                return '{}:{}'.format(self._nsmap_r[ns], name)
            else:
                return name
        else:
            return ''

    def _path_split_attr(self, path):
        # split the path to xpath and attrib
        path_attr_pattern = re.compile(r'(.*)\[@([^\]]*)\]$')
        m = path_attr_pattern.match(path)
        if m:
            real_path, attr = m.groups()
        else:
            real_path = path
            attr = None
        return real_path, attr

    def _path_split_seq(self, path):
        # split the path to path and sequence number
        p = re.compile(r'(.*)\[(\d+)\]$')
        m = p.match(path)
        if m:
            # with sequence number
            real_path, seq = m.groups()
            seq = int(seq)
        else:
            real_path = path
            seq = None
        # logger.debug('Sequence number is {}'.format(seq))
        return real_path, seq

    def _get_node_with_ns(self, path):
        # get the node with the namespace in tag
        if not self.root.nsmap:
            return None
        search = ''
        for seg in path.split('/')[1:]:
            if seg[-1] == ']':
                # with a sequence number
                name, _, seq = seg.partition('[')
                search += "/*[name()='{}'][{}".format(name, seq)
            else:
                search += "/*[name()='{}']".format(seg)
        logger.debug('Node search string is: {}'.format(search))
        l = self.etree.xpath(search)
        return l[0] if l else None

    def _get_node_detail(self, node):
        '''extra node tag, text, attribute and count of child'''
        if node is not None:
            tag = self._rm_prefix(node.tag) if self._nsmap_r else node.tag
            text = node.text.strip() if node.text else ''
            attrib = node.attrib
            count_child = len(node)
            return tag, text, attrib, count_child
        return None

    def _walk_node_list(self, node, prefix='', counter=1):
        '''
            This function is the recursive to get node list
            Needed only for xml with namespaces
        '''
        tag, text, attrib, count_child = self._get_node_detail(node)
        if counter == 1:
            new_prefix = prefix + '/' + tag
            sib = node.getnext()
            while sib != None:
                if sib.tag is etree.Comment:
                    sib = sib.getnext()
                    continue
                if self._rm_prefix(sib.tag) == tag:
                    new_prefix = new_prefix + '[1]'
                    break
                sib = sib.getnext()
        else:
            new_prefix = prefix + '/' + tag + '[{}]'.format(str(counter))
        self._dict[new_prefix] = (tag, text, attrib, count_child)
        self._node_map[new_prefix] = node
        counters = {}

        for ch in node.getchildren():
            if ch.tag is etree.Comment:
                continue
            tag = self._rm_prefix(ch.tag)
            counters[tag] = counters.setdefault(tag, 0) + 1
            self._walk_node_list(ch, prefix=new_prefix, counter=counters[tag])

    def _get_parent_name(self, path):
        parent, _, tag = path.rpartition('/')
        if tag.startswith('comment()'):
            return ''
        return parent

    def _get_node_list(self):
        '''
            Generate the _dict
            _dict: OrderedDict
                key: xpath
                value: [tag, text, attrib, child_count]
        '''
        self._dict = OrderedDict()
        self._node_map = {}
        if self.root.nsmap:
            '''
                etree.getpath returns weird string wchich cannot be used for etree.xpath to lookup
                the node.
                So here need to use self._walk_node_list()
            '''
            self._walk_node_list(self.root)
        else:
            for ch in self.root.iter():
                if ch.tag is etree.Comment:
                    continue
                path = self.etree.getpath(ch)
                self._dict[path] = self._get_node_detail(ch)
                self._node_map[path] = ch
        for name in self._dict.keys():
            parent = self._get_parent_name(name)
            if not parent:
                continue
            else:
                self._children_count[parent] = self._children_count.setdefault(parent, 0) + 1
        
    def _get_node(self, path):
        if self.root.nsmap:
            return self._get_node_with_ns(path)
        else:
            l = self.etree.xpath(path)
            if l:
                return l[0]
            else:
                logger.debug('Not found: {}'.format(path))
                return None
    # alias of get_node
    get_node = _get_node

    def get_path(self, node):
        # Find full path of node
        for path, n in self._node_map.items():
            if n == node:
                return path
        else:
            return ''

    # def _get_siblings(self, path):
    #     real_path, seq = self._path_split_seq(path)
    #     len_rp = len(real_path)
    #     ret = []
    #     if seq:
    #         for k, v in self._dict.items():
    #             if k == path:
    #                 continue
    #             if k.startswith(real_path) and '/' not in k[len_rp:]:
    #                 ret.append(k)
    #     return ret
                
    def _match_attr(self, attrib1, attrib2, group_id_keys:tuple):
        for key in group_id_keys:
            if key in attrib1 and key in attrib2 and attrib1.get(key) == attrib2.get(key):
                return True
        return False

    def _dict_diff(self, d1, d2, include_ok=True):
        # assume d1 and d2 are dict
        all_keys = uniq_seq_merge(list(d1.keys()), list(d2.keys()))
        diff = []
        for key in all_keys:
            if key in d1 and key not in d2:
                diff.append(('Removed', key, d1.get(key), ''))
            elif key not in d1 and key in d2:
                diff.append(('New', key, '', d2.get(key)))
            else:
                v1 = d1.get(key)
                v2 = d2.get(key)
                if v1 == v2:
                    if include_ok:
                        diff.append(('OK', key, v1, v2))
                else:
                    diff.append(('Change', key, v1, v2))
        return diff

    def __getitem__(self, path):
        return self._dict.get(path, None)

    def _list_children(self, parent_path, tag=''):
        if self[parent_path][-1] == 0:
            return []
        ret = []
        prefix = '/'.join((parent_path, tag))
        l_p = len(prefix)
        for k, v in self._dict.items():
            if k.startswith(prefix) and '/' not in k[l_p:]:
                ret.append(k)
        return ret

    def _get_match_node(self, path, node_match_matrix):
        # to find the correct node to match
        # For example, /groups/group[3] matches the /groups/group[4]
        # Then the children need to compare to each other's children
        # logger.debug('Looking for {} in {}'.format(path, node_match_matrix))
        for p1, p2 in node_match_matrix[::-1]:
            if path == p1:
                return p2
        return None

    def _get_attr(self, path, attr):
        return self._dict[path][2].get(attr, None)

    def _get_children_count(self, path, obj):
        if path in obj._dict:
            return obj._dict[path][-1]
        else:
            return 0

    def _group_node_match(self, parent_path, tag, xc2, node_match_matrix, id_field='id'):
        '''
            _group_node_match: use only when compare two instances
            args:
                parent_path: the group's parent path
                tag: the shared tag of group nodes
                xc2: the instance will be compared
                node_match_matrix: the node match matrix
                id_field: the atrribute to identify the node
        '''
        match = []
        unmatched = []
        id_map = {}
        ppath2 = self._get_match_node(parent_path, node_match_matrix)
        logger.debug('ppath2 is {}'.format(ppath2))
        if ppath2 == None:
            ppath2 = parent_path
        for path in xc2._list_children(ppath2, tag):
            _, seq = xc2._path_split_seq(path)
            node_id = xc2[path][2][id_field]
            id_map[node_id] = seq
        for path in self._list_children(parent_path, tag):
            _, seq = self._path_split_seq(path)
            node_id = self[path][2][id_field]
            if node_id in id_map:
                p2 = '{}/{}[{}]'.format(ppath2, tag, id_map[node_id])
                match.append((path, p2))
                logger.debug('Group {}: {} matched {}'.format(parent_path, path, p2))
                if p2 != path:
                    node_match_matrix.append((path, p2))
                id_map.pop(node_id)
            else:
                logger.debug('Group {}: no match found for {}'.format(parent_path, seq))
                unmatched.append(path)
        unmatched2 = ['{}/{}[{}]'.format(ppath2, tag, seq) for seq in id_map.values()]
        return match, unmatched, unmatched2

    def _mark_children(self, path, status, obj, node_compared):
        # mark all children to one single status
        # this can be used to mark all children of a Removed or New node to the same status
        ret = []
        logger.debug('Marking {}\'s sub nodes to be {}'.format(path, status))
        for sub_path in obj._dict.keys():
            if sub_path.startswith(path):
                _, text, attr, _ = obj[sub_path]
                node_compared.append(sub_path)
                logger.debug('Marking {} to {}'.format(sub_path, status))
                if status == 'New':
                    p1, p2 = '', sub_path
                    t1, t2 = '', text
                else:
                    p1, p2 = sub_path, ''
                    t1, t2 = text, ''
                ret.append((p1, status, t1, t2, '', p2))
                for key, value in attr.items():
                    if status == 'New':
                        p1, p2 = '', sub_path
                        v1, v2 = '', value
                    else:
                        p1, p2 = sub_path, ''
                        v1, v2 = value, ''
                    ret.append((p1, status, key, v1, v2, p2))
        # logger.debug('Diffs before return {}'.format(ret))
        return ret

    def comp(self, filename: str, group_id_field_map=None, include_ok=True):
        '''
            Compare with another xml file
            args:
                filename: file name
                group_id_field_map: a dict contains the id field of each group of sub nodes
                    The format should be this way: {'bean': 'id', 'Field': 'index', 'module': 'name'}
                    The key is the tag name and value should be the attribute to distinguish the node
                include_ok: Whether the return values contains the same content
            return:
                list of changes
        '''
        logger.debug('Compare to {}'.format(filename))
        if group_id_field_map == None:
            group_id_field_map = {}
        xc2 = self.__class__(filename)
        changes = []
        # node_match_matrix
        # This is designed for the nodes have multiple children with same tag but different
        # attributes
        # In some cases the order of childs may vary but actually the node can find equivelent
        # node in the second file. Keeping this matrix to avoid the accuracy problem due to
        # sequence
        # empty = ('', '', {}, 0)
        node_match_matrix = []
        # store the nodes already processed
        node_compared1 = []
        node_compared2 = []

        # check all nodes in self
        for path in self._dict.keys():
            logger.debug('Comparing my path {}'.format(path))
            if path in node_compared1:
                logger.debug('Already compared. Skip {}'.format(path))
                continue
            # logger.debug(node_compared1)
            # logger.debug(node_compared2)
            real_p, seq = self._path_split_seq(path)
            parent = self._get_parent_name(path)
            if not seq:
                # the path is not ending with a sequence number, means not in a group
                logger.debug('Not in a group')
                ppath2 = self._get_match_node(parent, node_match_matrix)
                if ppath2 == None:
                    path2 = path
                else:
                    path2 = path.replace(parent, ppath2)
                    if self._get_children_count(path, self) > 0:
                        node_match_matrix.append((path, path2))
                if path2 in xc2._dict:
                    logger.debug('Compare {} to {}'.format(path, path2))
                    left, right = self[path], xc2[path2]
                    diffs = self._node_comp(left, right, path, path2, include_ok=include_ok)
                    changes.extend(diffs)
                    node_compared1.append(path)
                    node_compared2.append(path2)
                else:
                    # not in xc2, need to mark as 'Removed'
                    diffs = self._mark_children(path, 'Removed', self, node_compared1)
                    changes.append(diffs)
                    node_compared1.append(path)
            else:
                # the path ends with a sequence number, means in a group
                logger.debug('In group: {}'.format(parent))
                _, _, tag = real_p.rpartition('/')
                if tag not in group_id_field_map:
                    raise KeyError('Please indicate id field for tag "{}" to group_id_field_map'.format(tag))
                matched, unmatched1, unmatched2 = self._group_node_match(
                    parent, tag, xc2, node_match_matrix, group_id_field_map[tag]
                )
                # logger.debug('Node match matrix: {}'.format(node_match_matrix))
                for p1, p2 in matched:
                    logger.debug('Comparing {} vs {}'.format(p1, p2))
                    diffs = self._node_comp(self[p1], xc2[p2], p1, p2, include_ok)
                    node_compared1.append(p1)
                    node_compared2.append(p2)
                    # logger.debug('Current diffs: {}'.format(diffs))
                    changes.extend(diffs)
                for p1 in unmatched1:
                    logger.debug('No match found for {} in {}. Mark as Removed'.format(p1, xc2.filename))
                    diffs = self._mark_children(p1, 'Removed', self, node_compared1)
                    # logger.debug('Removed diffs: {}'.format(diffs))
                    node_compared1.append(p1)
                    changes.extend(diffs)
                for p2 in unmatched2:
                    logger.debug('No match found for {} in self, Mark as New'.format(p2))
                    diffs = self._mark_children(p2, 'New', xc2, node_compared2)
                    # logger.debug('New diffs: {}'.format(diffs))
                    node_compared2.append(p2)
                    changes.extend(diffs)
        for path in xc2._dict.keys():
            if path in node_compared2:
                logger.debug('Already compared. Skip {}'.format(path))
                continue
            diffs = self._mark_children(path, 'New', xc2, node_compared2)
            changes.extend(diffs)

        return changes
                
    def _list_group_node(self, path):
        p = re.compile(r'(.*)\[(\d+)\]$')
        m = p.match(path)
        if m:
            # with sequence number
            real_path, seq = m.groups()
            seq = int(seq)
        return self.etree.xpath(real_path)

    def _node_comp(self, data_node1, data_node2, path1, path2='', include_ok=True):
        '''
            Compare data two nodes
            args:
                data_node1: data of node1 with path1
                data_node2: data of node2 with path2
                path1: the node path of self
                path2: the node path of the file to be compared
                include_ok: whether to inlucde values with no difference
        '''
        _, text1, attr1, _ = data_node1
        _, text2, attr2, _ = data_node2
        changes = []
        if not path2:
            path2 = path1
        if text1 != text2:
            logger.debug('Text difference: {} - {}'.format(text1, text2))
            changes.append((path1, 'Change', '', text1, text2, path2))
        elif text1 != '' and include_ok:
            changes.append((path1, 'OK', '', text1, text1, path2))
        attrib_diff = self._dict_diff(attr1, attr2, include_ok)
        if attrib_diff:
            logger.debug('Attrib diff: {}'.format(attrib_diff))
            for diff in attrib_diff:
                ch, key, v1, v2 = diff
                if ch == 'OK':
                    if include_ok:
                        changes.append((path1, ch, key, v1, v2, path2))
                else:
                    changes.append((path1, ch, key, v1, v2, path2))
        
        return sorted(changes, key=lambda t:t[0])


    def set(self, path, value):
        real_path, attr = self._path_split_attr(path)
        node = self._get_node(real_path)
        if node is None:
            raise KeyError('Invalid path: {}'.format(path))
        if node is not None:
            if attr:
                # set attrib
                node.set(attr, value)
                self[real_path][2][attr] = value
            else:
                # set text
                if value:
                    node.text = value
                    d = self[real_path]
                    self._dict[real_path] = (d[0], value, d[2], d[3])
        else:
            # cannot find or create the node
            logger.warn('Unable to find or create node')

    def set_attr(self, path, attr, value):
        if attr == '':
            self.set(path, value)
        else:
            new_path = '{}[@{}]'.format(path, attr)
            self.set(new_path, value)

    def get(self, path):
        real_path, attr = self._path_split_attr(path)
        # node = self._get_node(real_path)
        if real_path not in self._dict:
            raise KeyError('No xpath found: {}'.format(path))
        attrib = self[real_path][2]
        if attr:
            if attr not in attrib:
                raise KeyError('No attrib found: {}'.format(path))
            return attrib.get(attr)
        else:
            return self[real_path][1]

    def add_node(self, path:str):
        # if self._get_node(path) is not None:
        #     logger.info('Node exists')
        #     return
        # check if path with sequence number
        real_path, seq = self._path_split_seq(path)
        if seq:
            seq = int(seq)
            parent_path = self._get_parent_name(real_path)
            tag = real_path.rpartition('/')[-1]
            if parent_path not in self._dict:
                raise ValueError('Wrong path: {}'.format(real_path))
            parent_node = self._get_node(parent_path)
            max_child_path = self._list_children(parent_path, tag)[-1]
            max_n = int(self._path_split_seq(max_child_path)[-1])
            element = etree.Element(tag)
            if seq <= max_n:
                parent_node.insert(seq, element)
            else:
                parent_node.append(element)
        else:
            parent_path, _, tag = path.rpartition('/')
            parent_node = self._get_node(parent_path)
            element = etree.Element(tag)
            parent_node.append(element)
        self._get_node_list()
        return element

    def del_node(self, path:str):
        '''delete a node'''
        # delete it's children
        node = self._get_node(path)
        for child in node.getchildren():
            self.del_node(self.get_path(child))
        # delete node from etree
        parent = self._get_parent_name(path)
        parent_node = self.get_node(parent)
        parent_node.remove(node)
        # delete from self._dict and self._node_map
        self._get_node_list()

    def save(self, filename=''):
        # save to disk
        if filename == '':
            filename = self.filename
        logger.debug('Write to {}'.format(filename))
        open(filename, 'w').write(etree.tostring(self.root).decode())

    def walk(self, json=False):
        for k, v in self.get_dict().items():
            print('{}: {}'.format(k, v))

    def get_dict(self, json=False):
        ret = OrderedDict()
        for node_path, vs in self._dict.items():
            if node_path.split('/')[-1].startswith('comment()'):
                continue
            _, text, attrib, _ = vs
            ret[node_path] = text
            for key, value in attrib.iteritems():
                ret['{}[@{}]'.format(node_path, key)] = value
        return ret

    def update_from(self, filename, group_id_field_map=None):
        # Update current file from another file
        for path1, status, key, v1, v2, path2 in self.comp(filename, group_id_field_map, include_ok=False):
            if status == 'Change':
                action = status
            elif status == 'New':
                action = 'Add'
            elif status == 'Remove':
                logger.debug('Remove is not supported yet')
                continue
            logger.debug('{} {}: {} -> {}'.format(status, key, v1, v2))
            self.set(key, v2)

    def _node_validate(node, requirements: list, include_ok=True):
        ret = []
        for req in requirements:
            key = req.get('attrib')
            value = req.get('value')
            current_value = node.attrib.get(key)
            if current_value == None:
                change = 'Missing'
            elif current_value != value:
                change = 'NotComply'
            else:
                if include_ok == False:
                    continue
                change = 'OK'
            ret.append((key, value, current_value, change))
        return ret

    def validate(self, node_path, search_path, requirements: list, include_ok=False):
        '''
            Valid if self values match the requirements
            args:
                node_path: a XPath string can match a node
                search_path: a XPath search string that can to search nodes
                requirements: a list of value requirements
                    Example: [
                        {'attrib': 'key1', 'value': 'value1'},
                        {'attrib': 'key2', 'value': 'value2'},
                        {'attrib': 'key3', 'value': 'value3'},
                    ]
        '''
        ret = []
        if node_path == 'search' or node_path == '':
            logger.debug('Searching {}'.format(search_path))
            xpath = search_path
            nodes = self.search(search_path)
            if len(nodes) == 0:
                logger.debug('No node found for {}'.format(xpath))
                change = 'Missing'
                ret.append((xpath, '', '', '', 'NodeMissing'))
            # elif len(nodes) > 1:
            #     change = 'Deviation'
            #     ret.append((xpath, '', '', '', 'TooManyNode'))
            #     # raise ValueError('XPath matched two or more nodes: {}'.format(search_path))
            # else:
            #     logger.debug('Node found')
            #     node = nodes[0]
        else:
            node = self._get_node(node_path)
            xpath = node_path
            if node is None:
                change = 'Missing'
                ret.append((xpath, '', '', '', 'NodeMissing'))
            else:
                nodes = [node,]
        for node in nodes:
            path = self.get_path(node)
            for req in requirements:
                key = req.get('attrib')
                value = req.get('value')
                current_value = node.attrib.get(key)
                if current_value == None:
                    change = 'Missing'
                elif current_value != value:
                    change = 'NotComply'
                else:
                    if include_ok == False:
                        continue
                    change = 'OK'
                ret.append((path, key, value, current_value, change))
        if not ret:
            logger.debug('No compliance issue found')
        return ret

    def multi_set(self, search_path, attrib, value):
        for node in self.search(search_path):
            path = self.get_path(node)
            if attrib:
                path = '{}[@{}]'.format(path, attrib)
            self.set(path, value)