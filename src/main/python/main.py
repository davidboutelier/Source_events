from fbs_runtime.application_context.PyQt5 import ApplicationContext
from PyQt5.QtWidgets import QMainWindow, QFileDialog
from PyQt5.QtCore import QObject, QThread, pyqtSignal
from PyQt5.uic import loadUi
import time
import sys, os
import pandas as pd
from datetime import datetime
import numpy as np



class Worker(QObject):
    global proj_dict

    finished = pyqtSignal()
    countChanged = pyqtSignal(int)
    comm = pyqtSignal()

    def get_utm(self, file, line, station):
        positions = pd.read_csv(file) 
        
        Points = positions["Point"]
        eastings = positions["Easting"]                         # get an array of the eastings
        northings = positions["Northing"]                       # get an array of the northings
        elevations = positions["Elevation"]
        points = np.asarray(positions["Point"].values)
        index = np.argwhere(points == station)

        this_UTM = []                                           # create empty list to receive UTM coordinates
        if len(index) == 1:                                     # only populate the UTM list if there is only one idex
            index = index[0][0]
            this_UTM = [eastings[index], northings[index], elevations[index]]    # populate the list
            
        return this_UTM


    def find_FFID(self, file, line, station, sweep):
        if os.path.exists(file):
            theoretical_SP = file
            theo_df = pd.read_csv(theoretical_SP)
            theo_df_1 = theo_df[theo_df['Line'] == int(line)]

            if len(theo_df_1) == 0:
                #logger.error('Line no found in the theoretical file')
                R = 'nan'
            else:
                theo_df_2 = theo_df_1[theo_df_1['Point'] == int(station)]
                if len(theo_df_2) == 0:
                    #logger.error('Point no found in the theoretical file with line '+str(line)+' and point '+str(station))
                    R = 'nan'
                else:
                    theo_df_3 = theo_df_2[theo_df_2['Index'] == int(sweep)]
                    if len(theo_df_3) == 0:
                        #logger.error('Sweep no found in the theoretical file with line '+str(line)+' and point '+str(station)+'and sweep '+str(sweep))
                        R = ' nan'
                    else:
                        theo_FFID = theo_df_3['ID']
                        R = theo_FFID.values[0]

            return R




    def run(self):
        global proj_dict
        n_files_done = 0

        line = proj_dict['line']
        theoretical_file = proj_dict['theoretical']
        RTK_file = proj_dict['RTK']

        msg = 'Reading the triggers'
        proj_dict['msg'] = msg
        self.comm.emit()

        if proj_dict['timebox'] == 'RTM':
            df_boom = pd.read_csv(proj_dict['times'])
            list_boom_sec = []
            list_boom_all = []

            n_done = 0
            n_total = len(df_boom)

            for index, row in df_boom.iterrows():
                boom_datetime = datetime.strptime(row['Time'].lstrip(), '%Y-%m-%d %H:%M:%S.%f')
                boom_year = boom_datetime.year
                boom_month = boom_datetime.month
                boom_day = boom_datetime.day
                boom_hour = boom_datetime.hour
                boom_minute = boom_datetime.minute
                boom_second = boom_datetime.second
                boom_microsecond = boom_datetime.microsecond
                boom_longitude = row['Lon(deg E)']
                boom_latitude = row['Lat(deg N)']

                boom_sec = (int(boom_day)-1)*24*3600 + int(boom_hour)*3600 + int(boom_minute)*60 +  float(int(boom_second) + int(boom_microsecond)/1000000)
                list_boom_sec.append(boom_sec)
                this_line = int(boom_year), int(boom_month), int(boom_day),  int(boom_hour), int(boom_minute), float(int(boom_second) + int(boom_microsecond)/1000000), float(boom_longitude), float(boom_latitude)
                list_boom_all.append(this_line)

                n_done += 1 
                percent = int(100 * n_done / n_total)
                self.countChanged.emit(percent)

            array_trig_time_sec = np.asarray(list_boom_sec)
            array_trig_time_all = np.asarray(list_boom_all)

            msg = 'Reading the logs'
            proj_dict['msg'] = msg
            self.comm.emit()
            self.countChanged.emit(0)    # reset the progress bar for the next task

            log_df = pd.read_csv(proj_dict['log'])
            log_df.reset_index

            # create header for new file
            header_line = 'FFID, Line, Point, Index, Code, UTCDate, UTCTime, Eastings, Northings, Elevation, UpholeTime_ms, comments' 

            outfile = os.path.join(proj_dict['destination'], 'source_events.csv')
            compfile = os.path.join(proj_dict['destination'], 'source_events_comp.csv')

            with open(outfile,'w') as f:
                f.write(header_line)
                f.write('\n')

                header_line2 = 'line, station, sweep, log_day, log_month, log_year, log_hour, log_minute, log_second, index_trigger, error, trigger_year, trigger_month, trigger_day, trigger_hour, trigger_minute, trigger_second, longitude, latitude'
        
                with open(compfile, 'w') as f2:
                    f2.write(header_line2)
                    f2.write('\n')

                    n_done = 0
                    n_total = len(log_df)

                    for index, row in log_df.iterrows():
                        station = row['Station']
                        sweep = row['Sweep']
                        
                        FFID = self.find_FFID(theoretical_file, line, station, sweep)
                        UTM = self.get_utm(RTK_file, line, int(station))

                        log_datetime = datetime.strptime(row['Time'].lstrip(), '%d/%m/%Y %H:%M:%S')
                        log_year = log_datetime.year
                        log_month = log_datetime.month
                        log_day = log_datetime.day
                        log_hour = log_datetime.hour
                        log_minute = log_datetime.minute
                        log_second = log_datetime.second
                        log_microsecond = log_datetime.microsecond

                        log_s_only = (int(log_day)-1) * 24*3600 +int(log_hour)*3600 + int(log_minute)*60 + float(log_second+log_microsecond/1000000) 
                        
                        diff_array = array_trig_time_sec - log_s_only
                        min_diff = np.min(np.abs(diff_array))
                        index_min_diff = np.argmin(np.abs(diff_array))

                        
                        sec = float(array_trig_time_all[index_min_diff, 5]) + 0.000450     # add the 450 microseconds
                        if sec > 60:
                            print('seconds greater than 60')
                            sec = sec - 60
                            # add a minute
                            array_trig_time_all[index_min_diff, 4] = array_trig_time_all[index_min_diff, 4] + 1

                            if array_trig_time_all[index_min_diff, 4] == 60:
                                array_trig_time_all[index_min_diff, 4] = 0
                                # add on extra hour
                                array_trig_time_all[index_min_diff, 3] = array_trig_time_all[index_min_diff, 3] + 1

                                if array_trig_time_all[index_min_diff, 3] == 24:
                                    array_trig_time_all[index_min_diff, 3] = 0  # reset the hour    
                                    log_day = log_day + 1

                        UTCDate = f"{log_year:04d}"+'-'+f"{log_month:02d}"+'-'+f"{log_day:02d}"

                        sec = "{:0>9.6f}".format(sec)
                        UTCTime = f'{array_trig_time_all[index_min_diff, 3]:02.0f}'+':'+f'{array_trig_time_all[index_min_diff, 4]:02.0f}'+':'+sec
                        code = proj_dict['code']

                        f.write(str(FFID)+','+str(int(line))+','+str(station)+','+str(sweep)+','+code+','+UTCDate+','+UTCTime+','+f'{UTM[0]:06.3f}'+','+f'{UTM[1]:07.3f}'+','+f'{UTM[2]:03.3f}'+', 0,')
                            
                        f.write('\n')

                        f2.write(str(line)+', '+f'{station:4.0f}'+', '+ f'{sweep:1.0f}'+', '+str(log_day)+', '+str(log_month)+', '+str(log_year)+', '+str(log_hour)+', '+str(log_minute)+', '+str(log_second)+','+f'{index_min_diff:4.0f}'+', '+f'{min_diff:3.4f}'+', '+f'{array_trig_time_all[index_min_diff, 0]:02.0f}'+', '+f'{array_trig_time_all[index_min_diff, 1]:02.0f}'+', '+f'{array_trig_time_all[index_min_diff, 2]:4.0f}'+', '+f'{array_trig_time_all[index_min_diff, 3]:02.0f}'+', '+f'{array_trig_time_all[index_min_diff, 4]:02.0f}'+', '+f'{array_trig_time_all[index_min_diff, 5]:02.8f}'+','+f'{array_trig_time_all[index_min_diff, 6]:-03.8f}'+','+f'{array_trig_time_all[index_min_diff, 7]:-03.8f}')
                        f2.write('\n')
                        
                        n_done +=1
                        percent = int(100 * n_done/n_total)
                        self.countChanged.emit(percent)

        self.finished.emit()


class SourceEvents(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setupUI()

    def setupUI(self):  
        loadUi(main_ui, self)  
        self.setWindowTitle('Seismic Source Events 1.0.2')
        self.setContentsMargins(12,6,12,6)

        self.buttonBox.accepted.connect(self.find_events)
        self.buttonBox.rejected.connect(self.close)

        self.theoretical_button.clicked.connect(self.select_theoretical_file)
        self.RTK_button.clicked.connect(self.select_RTK_file)
        self.log_button.clicked.connect(self.select_log_file)
        self.times_button.clicked.connect(self.select_times_file)
        self.destination_button.clicked.connect(self.select_destination_folder)

    def select_destination_folder(self):
        destination_folder = QFileDialog.getExistingDirectory()  
        self.destination_line_edit.setText(destination_folder)

    def select_times_file(self):
        file = QFileDialog.getOpenFileName(self, "Open File",proj_dict['home'],"csv files (*.csv)")[0]
        self.times_line_edit.setText(file)

    def select_log_file(self):
        file = QFileDialog.getOpenFileName(self, "Open File",proj_dict['home'],"csv files (*.csv)")[0]
        self.log_line_edit.setText(file)

    def select_RTK_file(self):
        file = QFileDialog.getOpenFileName(self, "Open File",proj_dict['home'],"csv files (*.csv)")[0]
        self.RTK_line_edit.setText(file)

    def select_theoretical_file(self):
        file = QFileDialog.getOpenFileName(self, "Open File",proj_dict['home'],"csv files (*.csv)")[0]
        self.theoretical_line_edit.setText(file)
        
    def find_events(self):
        ready = False 

        theoretical_file = self.theoretical_line_edit.text()
        RTK_file = self.RTK_line_edit.text()
        log_file = self.log_line_edit.text()
        times_file = self.times_line_edit.text()
        destination_folder = self.destination_line_edit.text()
        line = self.line_spinBox.value()
        sweep_code = self.sweep_comboBox.currentText()

        if(not (theoretical_file and not theoretical_file.isspace())):
            self.statusBar().showMessage("Error - theoretical file is empty", 2000)
        else:
            if(not (RTK_file and not RTK_file.isspace())):
                self.statusBar().showMessage("Error - RTK file is empty", 2000)
            else:
                if(not (log_file and not log_file.isspace())):
                    self.statusBar().showMessage("Error - log file is empty", 2000)
                else:
                    if(not (times_file and not times_file.isspace())):
                        self.statusBar().showMessage("Error - times file is empty", 2000)
                    else:
                        if(not (destination_folder and not destination_folder.isspace())):
                            self.statusBar().showMessage("Error - destination folder is empty", 2000)
                        else:
                            if not os.path.exists(theoretical_file):
                                self.statusBar().showMessage("Error - theoretical file is not found", 2000)
                            else:
                                if not os.path.exists(RTK_file):
                                    self.statusBar().showMessage("Error - RTK file is not found", 2000)
                                else:
                                    if not os.path.exists(log_file):
                                        self.statusBar().showMessage("Error - log file is not found", 2000)
                                    else:
                                        if not os.path.exists(times_file):
                                            self.statusBar().showMessage("Error - times file is not found", 2000)
                                        else:
                                            ready = True
                                            proj_dict['theoretical'] = theoretical_file
                                            proj_dict['RTK'] = RTK_file
                                            proj_dict['log'] = log_file
                                            proj_dict['times'] = times_file
                                            proj_dict['line'] = line
                                            proj_dict['code'] = sweep_code
                                            proj_dict['destination'] = destination_folder
                                            
                                            if self.RTM_radio.isChecked():
                                                proj_dict['timebox'] = 'RTM'
                                            else:
                                                proj_dict['timebox'] = 'verify'
                                            

        if ready:
            self.statusBar().showMessage("All required files found - starting search", 200000)
            self.thread = QThread()
            self.worker = Worker() 
            self.worker.moveToThread(self.thread)
            self.thread.started.connect(self.worker.run) 
            self.worker.finished.connect(self.complete)
            self.worker.finished.connect(self.thread.quit)
            self.worker.finished.connect(self.worker.deleteLater)
            self.thread.finished.connect(self.thread.deleteLater)
            self.worker.countChanged.connect(self.progress)
            self.worker.comm.connect(self.update_msg)
            self.thread.start()

    def update_msg(self):
        global proj_dict
        msg = proj_dict['msg']
        self.statusBar().showMessage(msg, 200000)
    
    def progress(self, value):
                self.progressBar.setValue(value)
                    
    def complete(self):
                self.statusBar().showMessage("process complete - all files merged", 200000)
                time.sleep(1)
                self.close_app()

    def close_app(self):    
                time.sleep(2)
                self.close()



                            
        

if __name__ == '__main__':
    proj_dict = {}
    proj_dict['home'] = os.path.expanduser("~")
    appctxt = ApplicationContext()       # 1. Instantiate ApplicationContext
    main_ui = appctxt.get_resource('seismic_events.ui')
    window = SourceEvents()
    window.show()
    exit_code = appctxt.app.exec()      # 2. Invoke appctxt.app.exec()
    sys.exit(exit_code)