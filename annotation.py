

def annotate(querySteps):
    length = len(querySteps)
    holdOutput = None
    output = None
    useHold = False
    while length >= 1:
        curStep = querySteps[length - 1]
        curLayer = int(querySteps[length - 1][0:2])
        aboveLayer = int(querySteps[length - 2][0:2])
        # print("length: "+ str(length)+" querystep: "+querySteps[length-1])

        if "Join" in curStep or "Loop" in curStep:  # for operations that need 2 inputs
            if useHold:
                print(curStep[2:] + " on " + holdOutput[2:] + " and on " + output[2:])
                useHold = False
            else:
                print(curStep[2:] + " on " + querySteps[length + 1][2:] + " and on " + output[2:])
                if holdOutput is not None:
                    # print(holdOutput[:2] + " " + curStep)
                    if int(holdOutput[:2]) == int(curStep[:2]):
                        # print("usehold is true")
                        useHold = True
            output = curStep
        else:  # for operations that need 1 or 0 inputs
            if holdOutput is not None:
                # print(holdOutput[:2] + " " + curStep)
                if int(holdOutput[:2]) == int(curStep[:2]):
                    useHold = True
            if curLayer < aboveLayer:  # check if need to store output for far future use
                print(curStep[2:])
                holdOutput = curStep
            else:  # output not needed in far future
                if "Hash" in curStep:  # for operation that takes 1 input
                    if output is not None:
                        print(curStep[2:] + " on " + output[2:])
                        output = curStep
                    else:  # for operation that takes 0 input
                        print(curStep[2:])
                        output = curStep
                else:
                    print(curStep[2:])
                    output = curStep

        length -= 1
