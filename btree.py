from dataclasses import dataclass
import struct
from typing import Type

from pager import Pager
from row import Row

@dataclass
class NodeValue:
    key: int
    row: Row

class BTreeNode:
    def __init__(self, leaf=False):
        self.leaf = leaf
        self.values: list[NodeValue] = []
        self.children: list[BTreeNode] = []

    @classmethod
    def serialize_and_store(cls, node: 'BTreeNode', pager: Pager, is_root=False):
        result = struct.pack('?I', node.leaf, len(node.values))
        
        for value in node.values:
            result += struct.pack('I', value.key)
            result += value.row.serialize()
        
        if not node.leaf:
            for child in node.children:
                page_num = BTreeNode.serialize_and_store(child, pager)
                result += struct.pack('I', page_num)
        
        # Store the serialized node in the pager and return the page number
        page_num = pager.get_unused_page_num()
        if is_root:
            page_num = 0

        pager.set_page(page_num, result)

        return page_num
    
    @classmethod
    def deserialize(cls, pager: Pager, page_num: int):
        page_data = pager.get_page(page_num)
        leaf, num_values = struct.unpack('?I', page_data[:8])
        node = cls(leaf=leaf)

        # Unpack values
        offset = 8
        for _ in range(num_values):
            key, = struct.unpack('I', page_data[offset:offset+4])
            offset += 4
            row = Row.deserialize(page_data[offset:offset+Row.ROW_SIZE])
            offset += Row.ROW_SIZE
            node.values.append(NodeValue(key, row))

        if not leaf:
            # Unpack child page numbers
            child_page_nums = struct.unpack(f'{num_values + 1}I', page_data[offset:offset + (num_values + 1) * 4])
            node.children = [cls.deserialize(pager, child_page_num) for child_page_num in child_page_nums]

        return node


class BTree:
    def __init__(self, order):
        self.root = BTreeNode(True)
        self.order = order

    def insert(self, key, row):
        root = self.root
        value = NodeValue(key, row)

        # leaf node is not full
        if len(root.values) != self.order:
            self._insert_non_full(root, value)
            return
        
        # leaf node is full and we need to split
        new_root = BTreeNode()
        self.root = new_root
        new_root.children.insert(0, root)
        self._split_child(new_root, 0)
        self._insert_non_full(new_root, value)


    def _find_index_for_new_key(self, keys: list[int], key: int):
        i = len(keys) - 1
        while i >= 0 and key < keys[i]:
            i -= 1
        return i


    def _insert_non_full(self, node: BTreeNode, value: NodeValue):
        i = len(node.values) - 1

        if node.leaf:
            node.values.append(None)
            while i >= 0 and value.key < node.values[i].key:
                node.values[i + 1] = node.values[i]
                i -= 1
            node.values[i + 1] = value
            return

        # when not a leaf node
        # find index where the new key will be inserted
        while i >= 0 and value.key < node.values[i].key:
            i -= 1

        i += 1
        if len(node.children[i].values) == (2 * self.order) - 1:
            self._split_child(node, i)
            if value.key > node.values[i].key:
                i += 1

        self._insert_non_full(node.children[i], value)

    def _split_child(self, node: BTreeNode, index: int):
        child = node.children[index]
        new_child = BTreeNode(child.leaf)

        node.children.insert(index + 1, new_child)
        node.values.insert(index, child.values[self.order - 1])

        child.values = child.values[0 : self.order - 1]
        new_child.values = child.values[self.order : (2 * self.order) - 1]

        if not child.leaf:
            new_child.children = child.children[self.order : 2 * self.order]
            child.children = child.children[0 : self.order]

    def search(self, key: int, node=None):
        if node is None:
            node = self.root
        i = 0
        while i < len(node.values) and key > node.values[i].key:
            i += 1
        if i < len(node.values) and key == node.values[i].key:
            return node.values[i].row
        elif node.leaf:
            return None
        else:
            return self.search(key, node.children[i])

    def print_tree(self):
        self._print_tree(self.root, "ROOT")

    def _print_tree(self, node, direction, indent=""):
        print(f"{indent}{direction}:", end=" ")
        print(" ".join([str(x.row) for x in node.values]))
        if len(node.children) > 0:
            for i, child in enumerate(node.children):
                child_direction = 'L' if i == 0 else 'M' if i < len(node.children) - 1 else 'R'
                self._print_tree(child, child_direction, indent + "  ")


if __name__ == "__main__":
    B = BTree(3)
    for i in range(0, 100, 3):
        row = Row(i, f"user{i}", f"user{i}@example.com")
        B.insert(i, row)

    print(f"{B.search(24)=}")
    print(f"{B.search(120)=}")

    pager = Pager()
    BTreeNode.serialize_and_store(B.root, pager, is_root=True)
    root = BTreeNode.deserialize(pager, 0)
    B.root = root
    B.print_tree()
