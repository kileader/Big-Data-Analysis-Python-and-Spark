import csv

class TrackStarLinearRegression:

    def __init__(self, fileName):
        self.fileName = fileName
    

    def read_Csv(self):
        runnersSpeeds = []
        runnersAges = []
        runnersSorts = []

        with open(self.fileName, newline='') as csv_file:
            reader = csv.reader(csv_file, delimiter=',')
            for row in reader:
                runnersSpeeds.append(float(row[0]))
                runnersAges.append(float(row[1]))
                runnersSorts.append(float(row[2]))
        
        return (runnersSpeeds, runnersAges, runnersSorts)