"""Tests for the ``merge_inputs`` helper functions."""

from __future__ import annotations

import os
import tempfile

from absl.testing import absltest

from scripts import merge_inputs


class LoadTableTest(absltest.TestCase):

  def test_load_table_parses_percentage_with_comma_decimal(self):
    csv_content = "fecha;descuento_cocinas\n24/08/23;5,9%\n25/08/23;4,5%\n"
    tmp_dir = tempfile.mkdtemp()
    path = os.path.join(tmp_dir, "data.csv")
    try:
      with open(path, "w", encoding="utf-8") as f:
        f.write(csv_content)

      df = merge_inputs.load_table(
          path, sep=";", decimal=",", date_column="fecha", thousands=None
      )

      self.assertListEqual(df["descuento_cocinas"].tolist(), [5.9, 4.5])
    finally:
      os.remove(path)
      os.rmdir(tmp_dir)


if __name__ == "__main__":
  absltest.main()

