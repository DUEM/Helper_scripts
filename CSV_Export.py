"""Dump SQL database into CSV files, optionally limiting the range.

Usage:
    CSV_Export.py [-v] [-d|-w|-m <value>] [-e <end>]
    CSV_Export.py [-v] -s <start> [-e <end>]
    CSV_Export.py -h|--help

Options:
    -h --help     show this help message and exit
    --version     show version and exit
    -v --verbose  print status messages
    -d <value>    export data from the last <value> days
    -w <value>    export data from the last <value> weeks
    -m <value>    export data from the last <value> months
    -s <start>    export data from <start> (YYYY/MM/DD)
    -e <end>      export data until <end> (YYYY/MM/DD)

"""
import mysql.connector
import csv
from docopt import docopt
# mysql config
conninfo = {"user": "root",
            "password": "dusc2015",
            "host": "127.0.0.1",
            "database": "2016test"}

motorpairs = (("busCurrent", "busVoltage"),
              ("vehicleVelocity", "motorVelocity"),
              ("phaseACurrent", "phaseBCurrent"),
              ("vectVoltReal", "vectVoltImag"),
              ("vectCurrReal", "vectCurrImag"),
              ("backEMFReal", "backEMFImag"),
              ("fifteenVsupply", "onesixfiveVsupply"),
              ("twofiveVsupply", "onetwoVsupply"),
              ("fanSpeed", "fanDrive"),
              ("heatSinkTemp", "motorTemp"),
              ("airInletTemp", "processorTemp"),
              ("airOutletTemp", "capacitorTemp"),
              ("DCBusAmpHours", "Odometer"))

controlpairs = (("busCurrent", "motorCurrent"),
                ("motorVelocity", None))
cnx = mysql.connector.connect(**conninfo)
cursor = cnx.cursor()

if __name__ == '__main__':
    arguments = docopt(__doc__, version='0.1.0 (Coping Mechanism)')
    print(arguments)
    # exit()
    # Export motor data
    for data in motorpairs:
        query = "SELECT time, {0}, {1} FROM motorstate WHERE {0} IS NOT NULL OR {1} IS NOT NULL".format(*data)
        # query = "SELECT time, {0}, {1} FROM motorstate".format(data[0], data[1])
        # print(query)
        cursor.execute(query)
        csvname = "csvfiles/{0}_{1}.csv".format(data[0], data[1])
        with open(csvname, 'w', newline='\r\n') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(('time',) + data)
            val0 = None
            val1 = None
            print(csvname)
            for row in cursor:
                if row[1] is not None:
                    val0 = row[1]
                if row[2] is not None:
                    val1 = row[2]
                writer.writerow([row[0], val0, val1])

    # Export controls data
    data = ("setMotorCurrent", "setMotorVelocity")
    query = "SELECT time, {0}, {1} FROM controls WHERE {0} IS NOT NULL OR {1} IS NOT NULL".format(*data)
    # print(query)
    cursor.execute(query)
    csvname = "csvfiles/{0}_{1}.csv".format(data[0], data[1])
    with open(csvname, 'w', newline='\r\n') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(('time',) + data)
        val0 = None
        val1 = None
        print(csvname)
        for row in cursor:
            if row[1] is not None:
                val0 = row[1]
            if row[2] is not None:
                val1 = row[2]
            writer.writerow([row[0], val0, val1])

    # Export GPS data
    data = ("lat", "lon", "alt", "track", "speed", "climb")
    query = "SELECT time, {0}, {1}, {2}, {3}, {4}, {5} FROM gps_tpv".format(*data)
    # print(query)
    cursor.execute(query)
    csvname = "csvfiles/gps.csv"
    with open(csvname, 'w', newline='\r\n') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(('time',) + data)
        print(csvname)
        for row in cursor:
            writer.writerow(row)

    # Export battery data
    # determine module numbers
    modquery = "SELECT DISTINCT modID FROM batteries ORDER BY modID"
    cursor.execute(modquery)
    modIDs = []
    for row in cursor:
        modIDs.append(row[0])
    cells = tuple(range(4*len(modIDs)))
    data = ("modID", "cellV0", "cellV1", "cellV2", "cellV3")
    query = "SELECT time, {0}, {1}, {2}, {3}, {4} FROM batteries".format(*data)
    # print(query)
    cursor.execute(query)
    csvname = "csvfiles/batteries.csv"
    with open(csvname, 'w', newline='\r\n') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(('time',) + cells)
        print(csvname)
        voltages = [None] * len(cells)
        for row in cursor:
            cellnum = 4*modIDs.index(row[1])
            voltages[cellnum:cellnum+4] = row[2:]
            if row[1] == max(modIDs):
                writer.writerow([row[0]] + voltages)

    cursor.close()
    cnx.close()
