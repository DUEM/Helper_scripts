import mysql.connector
import csv
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


for data in motorpairs:
    # query = "SELECT time, {0}, {1} FROM motorstate WHERE {0} IS NOT NULL OR {1} IS NOT NULL".format(data[0], data[1])
    query = "SELECT time, {0}, {1} FROM motorstate".format(data[0], data[1])
    print(query)
    cursor.execute(query)
    csvname = "csvfiles/{0}_{1}.csv".format(data[0], data[1])
    with open(csvname, 'w', newline='\r\n') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['time', data[0], data[1]])
        val0 = None
        val1 = None
        print(csvname)
        for row in cursor:
            if row[1] is not None:
                val0 = row[1]
            if row[2] is not None:
                val1 = row[2]
            writer.writerow([row[0], val0, val1])


data = ("setBusCurrent", "setMotorCurrent", "setMotorVelocity")

query = "SELECT time, {0}, {1}, {2} FROM controls".format(data[0], data[1], data[2])
print(query)
cursor.execute(query)
csvname = "csvfiles/{0}_{1}_{2}.csv".format(data[0], data[1], data[2])
with open(csvname, 'w', newline='\r\n') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['time', data[0], data[1], data[2]])
    val0 = None
    val1 = None
    val2 = None
    print(csvname)
    for row in cursor:
        if row[1] is not None:
            val0 = row[1]
        if row[2] is not None:
            val1 = row[2]
        if row[3] is not None:
            val2 = row[3]
        writer.writerow([row[0], val0, val1, val2])
data = ("time", "lat", "lon", "alt", "track", "speed", "climb")
query = "SELECT time, {0}, {1}, {2}, {3}, {4}, {5} FROM gps_tpv".format(data[0], data[1], data[2], data[3], data[4], data[5])
print(query)
cursor.execute(query)
csvname = "csvfiles/gps.csv"
with open(csvname, 'w', newline='\r\n') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['time',data[0], data[1], data[2], data[3], data[4], data[5]])
    val0 = None
    val1 = None
    val2 = None
    val3 = None
    val4 = None
    val5 = None
    print(csvname)
    for row in cursor:
        writer.writerow(row)

cursor.close()
cnx.close()
