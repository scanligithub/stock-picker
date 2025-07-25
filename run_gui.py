# run_gui.py

from gui.main_window import StockKLineViewer
from PyQt5.QtWidgets import QApplication
import sys

if __name__ == '__main__':
    app = QApplication(sys.argv)
    window = StockKLineViewer()
    window.show()
    sys.exit(app.exec_())