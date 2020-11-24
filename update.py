import pandas as pd
import datetime
import pyodbc
import logging
import sqlalchemy
import sqlalchemy_access as sa_a
import sqlalchemy_access.pyodbc as sa_a_pyodbc
from sqlalchemy.types import String
from sqlalchemy import MetaData
from sqlalchemy.engine.url import URL
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from PyQt5 import QtCore, QtGui, QtWidgets
from PyQt5.QtWidgets import QApplication, QWidget, QInputDialog, QLineEdit, QFileDialog

def drop_table(table_name):
    conn = engine.raw_connection()
    crsr = conn.cursor()
    table_name = 'Shipment_Detail_Temp'
    # dict comprehension: {ordinal_position: col_name}
    pk_cols = {row[7]: row[8] for row in crsr.statistics(table_name) if row[5]=='PrimaryKey'}
    #print(pk_cols)  # e.g., {1: 'InvID', 2: 'LineItem'}
    base = declarative_base()
    #metadata = MetaData(engine, reflect=True)
    meta = MetaData()
    meta.reflect(bind=engine)
    table = meta.tables.get(table_name)
    if table is not None:
        logging.info(f'Deleting {table_name} table')
        base.metadata.drop_all(engine, [table], checkfirst=True)

def trim_all_columns(df2):
    trim_strings = lambda x: x.strip('"') if isinstance(x, str) else x
    return df2.applymap(trim_strings)
	
class Ui_MainWindow(object):
    def setupUi(self, MainWindow):
        MainWindow.setObjectName("MainWindow")
        MainWindow.resize(670, 118)
        self.centralwidget = QtWidgets.QWidget(MainWindow)
        self.centralwidget.setObjectName("centralwidget")
        self.button_browse = QtWidgets.QPushButton(self.centralwidget)
        self.button_browse.setGeometry(QtCore.QRect(450, 20, 104, 31))
        self.button_browse.setObjectName("button_browse")
        self.credit = QtWidgets.QLabel(self.centralwidget)
        self.credit.setGeometry(QtCore.QRect(470, 70, 201, 20))
        font = QtGui.QFont()
        font.setPointSize(7)
        self.credit.setFont(font)
        self.credit.setObjectName("credit")
        self.pathlabel = QtWidgets.QLabel(self.centralwidget)
        self.pathlabel.setGeometry(QtCore.QRect(20, 30, 131, 18))
        font = QtGui.QFont()
        font.setPointSize(9)
        self.pathlabel.setFont(font)
        self.pathlabel.setObjectName("pathlabel")
        self.button_apply = QtWidgets.QPushButton(self.centralwidget)
        self.button_apply.setGeometry(QtCore.QRect(560, 20, 104, 31))
        self.button_apply.setObjectName("button_apply")
        self.text_path_csv = QtWidgets.QPlainTextEdit(self.centralwidget)
        self.text_path_csv.setGeometry(QtCore.QRect(150, 20, 291, 31))
        self.text_path_csv.setObjectName("text_path_csv")
        MainWindow.setCentralWidget(self.centralwidget)
        self.statusbar = QtWidgets.QStatusBar(MainWindow)
        self.statusbar.setObjectName("statusbar")
        MainWindow.setStatusBar(self.statusbar)

        self.retranslateUi(MainWindow)
        QtCore.QMetaObject.connectSlotsByName(MainWindow)

    def retranslateUi(self, MainWindow):
        global _translate
        _translate = QtCore.QCoreApplication.translate
        MainWindow.setWindowTitle(_translate("MainWindow", "Window"))
        self.button_browse.setText(_translate("MainWindow", "Browse"))
        self.credit.setText(_translate("MainWindow", "Credit: "))
        self.pathlabel.setText(_translate("MainWindow", "Ship Notice Path"))
        self.button_apply.setText(_translate("MainWindow", "Apply"))
        self.button_browse.clicked.connect(self.pushButton_handler)
        self.button_apply.clicked.connect(self.apply_handler)
        
    def pushButton_handler(self):
        #print("Button Pressed")
        self.open_dialog_box()
        
    def open_dialog_box(self):
        #_translate = QtCore.QCoreApplication.translate
        filename = QFileDialog.getOpenFileName()
        global path 
        path = filename[0]
        self.text_path_csv.setPlainText(_translate("MainWindow", path))
        #print(path)
 
    def apply_handler(self):
        #print("Button Pressed")
        if "path" in globals():
            self.runsqlcomparison()
        else:
            self.text_path_csv.setPlainText(_translate("MainWindow", "Please Select File"))

    def runsqlcomparison(self):
        global engine
        engine = create_engine("access+pyodbc://shipment_ct")
        df1 = pd.read_sql(r'SELECT * FROM Shipment_Detail', con=engine, parse_dates={'shipment_closed_time': '%Y-%m-%d %H:%M:%S'})
        df1 = df1.filter(['shipment_no', 'shipment_closed_time'])
        d_parser = lambda x : datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S')
        df2 = pd.read_csv(path, parse_dates=['Ship Date'], date_parser=d_parser, sep='","', engine='python')
        df2 = trim_all_columns(df2)
        df2.rename({'"Shipment No' : 'shipment_no', 'Ship Date':'shipment_closed_time'}, axis='columns', inplace = True)
        df2 = df2.filter(['shipment_no', 'shipment_closed_time'])
        df1.update(df2, overwrite=False)
        duplicatevalue = df2.duplicated(subset=['shipment_no']).any()
        if duplicatevalue == True:
            #print("Duplicate shipment found. Please extract ship notice with criteria 'From site: 99%'", end='\n\n')
            self.text_path_csv.setPlainText(_translate("MainWindow", "Duplicate shipment found. Please extract with 'From site: 99%'"))
        else:
            conn = engine.raw_connection()
            crsr = conn.cursor()
            table_name = 'Shipment_Detail_Temp'
            # dict comprehension: {ordinal_position: col_name}
            pk_cols = {row[7]: row[8] for row in crsr.statistics(table_name) if row[5]=='PrimaryKey'}
            SQLdel = "DELETE * FROM Shipment_Detail_Temp"
            with engine.connect() as connection:
                result = connection.execute(SQLdel)
            #drop_table('Shipment_Detail_Temp')
            df1.to_sql('Shipment_Detail_Temp', con=engine, if_exists='append', index=False, dtype={'shipment_no': String})
            SQL = "UPDATE Shipment_Detail LEFT JOIN Shipment_Detail_Temp ON Shipment_Detail.shipment_no = Shipment_Detail_Temp.shipment_no SET Shipment_Detail.shipment_closed_time = Shipment_Detail_Temp.shipment_closed_time"
            with engine.connect() as connection:
                result = connection.execute(SQL)
                result2 = connection.execute(SQLdel)
        
            #drop_table('Shipment_Detail_Temp')
            #print("Shipment Updated", end='\n\n')
            self.text_path_csv.setPlainText(_translate("MainWindow", "Shipment Updated"))
    
def main():    
    import sys        
    app = QtWidgets.QApplication(sys.argv)
    MainWindow = QtWidgets.QMainWindow()
    ui = Ui_MainWindow()
    ui.setupUi(MainWindow)
    MainWindow.show()
    sys.exit(app.exec_())
    x = input("...")    
    
if __name__ == "__main__":
    main()
    x
