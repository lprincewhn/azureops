import importlib.util
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).with_name("allocate_ri_difference.py")
SPEC = importlib.util.spec_from_file_location("allocate_ri_difference", MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


class VmResourceIdTests(unittest.TestCase):
    def test_accepts_virtual_machine_resource_id(self):
        resource_id = (
            "/subscriptions/sub/resourceGroups/rg/providers/"
            "Microsoft.Compute/virtualMachines/vm"
        )
        self.assertTrue(MODULE.is_vm_resource_id(resource_id))

    def test_accepts_virtual_machine_scale_set_resource_id(self):
        resource_id = (
            "/subscriptions/sub/resourceGroups/rg/providers/"
            "Microsoft.Compute/virtualMachineScaleSets/vmss"
        )
        self.assertTrue(MODULE.is_vm_resource_id(resource_id))

    def test_rejects_non_vm_resource_id(self):
        resource_id = (
            "/subscriptions/sub/resourceGroups/rg/providers/"
            "Microsoft.Storage/storageAccounts/account"
        )
        self.assertFalse(MODULE.is_vm_resource_id(resource_id))


if __name__ == "__main__":
    unittest.main()
