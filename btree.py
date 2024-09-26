import struct
from typing import Type

from pager import Pager


class BTreeNode:
    def __init__(self, leaf=False):
        self.leaf = leaf
        self.keys: list[int] = []
        self.children: list[BTreeNode] = []

    @classmethod
    def serialize_and_store(cls, node, pager: Pager, is_root=False):
        result = struct.pack('?I', node.leaf, len(node.keys))
        
        for key in node.keys:
            result += struct.pack('I', key)
        
        if not node.leaf:
            for child in node.children:
                page_num = BTreeNode.serialize_and_store(child, pager)
                result += struct.pack('I', page_num)
        
        # Pad the result to fill the page size
        page_size = pager.PAGE_SIZE
        result += b'\0' * (page_size - len(result))
        
        # Store the serialized node in the pager and return the page number
        page_num = pager.get_unused_page_num()
        if is_root:
            page_num = 0

        print(f"{page_num=}, for {node.keys=}")
        pager.set_page(page_num, result)

        return page_num
    
    @classmethod
    def deserialize(cls, pager: Pager, page_num: int):
        page_data = pager.get_page(page_num)
        leaf, num_keys = struct.unpack('?I', page_data[:8])
        node = cls(leaf=leaf)

        # Unpack keys
        keys_end = 8 + num_keys * 4
        keys = struct.unpack(f'{num_keys}I', page_data[8:keys_end])
        node.keys = list(keys)

        if not leaf:
            # Unpack child page numbers
            child_page_nums = struct.unpack(f'{num_keys + 1}I', page_data[keys_end:keys_end + (num_keys + 1) * 4])
            node.children = [cls.deserialize(pager, child_page_num) for child_page_num in child_page_nums]

        return node


class BTree:
    def __init__(self, order):
        self.root = BTreeNode(True)
        self.order = order

    def insert(self, key):
        root = self.root

        # leaf node is not full
        if len(root.keys) != self.order:
            self._insert_non_full(root, key)
            return
        
        # leaf node is full and we need to split
        new_root = BTreeNode()
        self.root = new_root
        new_root.children.insert(0, root)
        self._split_child(new_root, 0)
        self._insert_non_full(new_root, key)


    def _find_index_for_new_key(self, keys: list[int], key: int):
        i = len(keys) - 1
        while i >= 0 and key < keys[i]:
            i -= 1
        return i


    def _insert_non_full(self, node: BTreeNode, key: int):
        i = len(node.keys) - 1

        # when leaf node just insert the key
        if node.leaf:
            node.keys.append(0)
            while i >= 0 and key < node.keys[i]:
                i -= 1
            node.keys[i + 1] = key
            return


        # when not a leaf node
        # find index where the new key will be inserted
        while i >= 0 and key < node.keys[i]:
            i -= 1

        i += 1
        if len(node.children[i].keys) == (2 * self.order) - 1:
            self._split_child(node, i)
            if key > node.keys[i]:
                i += 1

        self._insert_non_full(node.children[i], key)

    def _split_child(self, node: BTreeNode, index: int):
        child = node.children[index]
        new_child = BTreeNode(child.leaf)

        node.children.insert(index + 1, new_child)
        node.keys.insert(index, child.keys[self.order - 1])

        child.keys = child.keys[0 : self.order - 1]
        new_child.keys = child.keys[self.order : (2 * self.order) - 1]

        if not child.leaf:
            new_child.children = child.children[self.order : 2 * self.order]
            child.children = child.children[0 : self.order]

    def search(self, key, node=None):
        if node is None:
            node = self.root
        i = 0
        while i < len(node.keys) and key > node.keys[i]:
            i += 1
        if i < len(node.keys) and key == node.keys[i]:
            return (node, i)
        elif node.leaf:
            return None
        else:
            return self.search(key, node.children[i])

    def print_tree(self):
        self._print_tree(self.root, 0)

    def _print_tree(self, node, direction, indent=""):
        print(f"{indent}{direction}:", end=" ")
        print(" ".join(map(str, node.keys)))
        if len(node.children) > 0:
            for i, child in enumerate(node.children):
                child_direction = 'L' if i == 0 else 'M' if i < len(node.children) - 1 else 'R'
                self._print_tree(child, child_direction, indent + "  ")


if __name__ == "__main__":
    B = BTree(3)
    for i in range(0,100,3):
        B.insert(i)


    pager = Pager()
    root = BTreeNode.deserialize(pager, 0)
    B.root = root
    B.print_tree()
