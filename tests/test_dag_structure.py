import unittest

from pypelines import Iterable, Map, Filter, StdOut, Assert

class TestDagStructure(unittest.TestCase):

    def test_depth_root_is_0(self):
        workflow = Iterable(range(1000))
        self.assertEqual(workflow.depth(), 0)

    def test_depth_second_level_is_1(self):
        workflow = Iterable(range(1000)) | Map(lambda x: x+1)
        self.assertEqual(workflow.depth(), 1)

    def test_depth_third_level_is_2(self):
        workflow = Iterable(range(1000)) | Map(lambda x: x+1) | StdOut()
        self.assertEqual(workflow.depth(), 2)


    def test_depth_third_level_2_two_branches(self):
        workflow = Iterable(range(1000))
        branch1 = workflow | Map(lambda x: x+1) | StdOut()
        branch2 = workflow | Map(lambda x: x+1) | StdOut()
        self.assertEqual(branch1.depth(), 2)
        self.assertEqual(branch2.depth(), 2)

    def test_dinasty_third_level(self):
        workflow = Iterable(range(1000)) | Map(lambda x: x+1) | StdOut()
        self.assertEqual(workflow.dinasty(), "Iterable/Map/StdOut")

    def test_dinasty_compose_dag(self):
        sub_workflow = Map(lambda x: x+1) | Map(lambda x: x+1) | Map(lambda x: x+1)
        workflow = Iterable(range(1000)) | sub_workflow | StdOut()
        self.assertEqual(workflow.dinasty(), "Iterable/Map/Map/Map/StdOut")

    def test_dinasty_third_level_2_two_branches(self):
        workflow = Iterable(range(1000))
        branch1 = workflow | Map(lambda x: x+1) | StdOut()
        branch2 = workflow | Filter(lambda x: x > 500) | StdOut()
        self.assertEqual(branch1.dinasty(), "Iterable/Map/StdOut")
        self.assertEqual(branch2.dinasty(), "Iterable/Filter/StdOut")

    def test_query_search_for_leaf(self):
        workflow = Iterable(range(1000))
        branch1 = workflow | Map(lambda x: x+1) | StdOut()
        self.assertAlmostEqual( workflow.query("Iterable/Map/StdOut").name(), "StdOut" )

    def test_query_search_for_leaf_on_two_branches(self):
        workflow = Iterable(range(10))
        branch1 = workflow | Map(lambda x: x+1) | StdOut()
        branch2 = workflow | Filter(lambda x: x > 5) | Assert(self, [6, 7, 8, 9])

        self.assertEqual(workflow.query("Iterable/Map/StdOut").name(), "StdOut")
        self.assertEqual(workflow.query("Iterable/Filter/Assert").name(), "Assert")

    def test_query_search_for_leaf_from_second_level(self):
        workflow = Iterable(range(10))
        map = workflow | Map(lambda x: x+1)
        stdout = map | StdOut()
        self.assertEqual(map.query("Map/StdOut").name(), "StdOut")

    def test_leafs_single_node_dag(self):
        workflow = Iterable(range(10))
        self.assertEqual(workflow.leafs(), [workflow])

    def test_leafs_2_nodes_dag(self):
        workflow = Iterable(range(10)) | Map(lambda x: x+1)
        self.assertEqual([n.name() for n in workflow.leafs()], ["Map"])

    def test_leafs_3_nodes_dag(self):
        workflow = Iterable(range(10)) | Map(lambda x: x+1) | StdOut()
        self.assertEqual([n.name() for n in workflow.leafs()], ["StdOut"])

    def test_leafs_2_breanches_balanced(self):
        workflow = Iterable(range(1000))
        branch1 = workflow | Map(lambda x: x+1) 
        branch2 = workflow | Filter(lambda x: x > 500) 
        self.assertEqual([n.name() for n in workflow.leafs()], ["Map", "Filter"])

    # def test_3_items(self):
    #     workflow = Iterable(range(1000)) 
    #     workflow / Map(lambda x: x+1) / StdOut

    #     for n in workflow.traverse_preorder():
    #         print(str(n))

    # def test_depth_root_node(self):
    #     workflow = Iterable(range(1000))
    #     map = workflow / Map(lambda x: x+1)
    #     stdout = map / StdOut
    #     self.assertEqual(workflow.depth(), 0)
    #     self.assertEqual(stdout.depth(), 2)

if __name__ == "__main__":
    unittest.main()
