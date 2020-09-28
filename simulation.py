import csv, sys, urllib.request, argparse


def main(file, server):
    processedData = []
    # uncomment below code to make url based csv file to work
    try:
        response = urllib.request.urlopen(file)
        lines = [l.decode('utf-8') for l in response.readlines()]
        csvData = csv.reader(lines, delimiter=',', quotechar='"')
        for i, row in enumerate(csvData):
            processedData.append(row)

        if server is not None and int(server) > 1:
            print(simulateManyServers(processedData, 3))
        else:
            print(simulateOneServer(processedData))

    except ValueError:
        print('Error processing the CSV file')
        sys.exit()
    # uncomment below code to make local file to work
    # try:
    #     with open('data.csv', newline='') as csvfile:
    #         csvData = csv.reader(csvfile, delimiter=',', quotechar='"')
    #         for i, row in enumerate(csvData):
    #             processedData.append(row)
    #
    #     if server is not None and int(server) > 1:
    #         print(simulateManyServers(processedData, 3))
    #     else:
    #         print(simulateOneServer(processedData))
    #
    # except ValueError:
    #     print('Error processing the CSV file')
    #     sys.exit()


def simulateOneServer(data):
    wait_time = []
    request_queue = []
    server = Server()
    currentTime = 0
    pos = 0

    while pos < len(data):
        if currentTime < int(data[pos][0]):
            currentTime += 1
        elif currentTime == int(data[pos][0]):
            newRequest = Request(currentTime, int(data[pos][2]))
            request_queue.append(newRequest)
            pos += 1
        if (not server.busy()) and len(request_queue) >= 1:
            nextRequest = request_queue.pop(0)
            wait_time.append(nextRequest.waitTime(currentTime))
            server.startNext(nextRequest)

        server.tick()

    return sum(wait_time) / len(wait_time)


def simulateManyServers(data, num):
    servers = [Server() for i in range(num)]
    wait_time = []
    request_queue = []
    currentTime = 0
    pos = 0
    stop = False

    while pos < len(data) and stop is not True:
        if currentTime < int(data[pos][0]):
            currentTime += 1
        elif currentTime == int(data[pos][0]):
            newRequest = Request(currentTime, int(data[pos][2]))
            request_queue.append(newRequest)
            pos += 1

        server_idle = False
        for i in servers:
            if (not i.busy()) and len(request_queue) == 0 and pos >= len(data) - 1:
                server_idle = True
            elif (not i.busy()) and len(request_queue) >= 1:
                nextRequest = request_queue.pop(0)
                wait_time.append(nextRequest.waitTime(currentTime))
                # print(currentTime, nextRequest.getStamp(),nextRequest.waitTime(currentTime))
                i.startNext(nextRequest)
            i.tick()
        stop = server_idle

    return sum(wait_time) / len(wait_time)


class Server:
    def __init__(self):
        self.currentTask = None
        self.timeRemaining = 0

    def tick(self):
        if self.currentTask is not None:
            self.timeRemaining = self.timeRemaining - 1
            if self.timeRemaining <= 0:
                self.currentTask = None

    def busy(self):
        if self.currentTask is not None:
            return True
        else:
            return False

    def startNext(self, newtask):
        self.currentTask = newtask
        self.timeRemaining = newtask.getEstimate()


class Request:
    def __init__(self, time, processTime):
        self.timestamp = time
        self.processTime = processTime

    def getStamp(self):
        return self.timestamp

    def getEstimate(self):
        return self.processTime

    def waitTime(self, currenttime):
        return currenttime - self.timestamp


parser = argparse.ArgumentParser()
parser.add_argument("--file")
parser.add_argument("--server")
args = parser.parse_args()
if not args.file:
    print('--file arg is required')
    sys.exit()
elif args.file and args.server:
    main(args.file, args.server)
else:
    main(args.file, 1)
