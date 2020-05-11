import os
import sys
import tempfile
import unittest
from datetime import datetime
from zipfile import ZipFile

from merlin.stage import Stage, StageType, StageOutputType


class StageTestCase(unittest.TestCase):

    def test_custom_stage(self):
        temp_dir = tempfile.mkdtemp()
        zip_filename = os.path.join(temp_dir, "test_lib.zip")
        with ZipFile(zip_filename, 'w') as zip_file:
            for folder, subfolders, filenames in os.walk("test/custom_test_stages"):
                for f in filenames:
                    source = os.path.join(folder, f)
                    arcname = f"custom_stages/{f}"
                    zip_file.write(filename=source, arcname=arcname)

        sys.path.append(zip_filename)
        s = Stage(
            execution_type=StageType.python,
            output_type=StageOutputType.view,
            py_mod="custom_stages.sample_stage",
            py_stage_args={'lookback_window': 5, 'start_date': datetime(2020, 1, 10, 0, 3)}
        )
        self.assertIsNotNone(s)


if __name__ == '__main__':
    unittest.main()
