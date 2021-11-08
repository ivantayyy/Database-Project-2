

def annotate(querySteps):
    length = len(querySteps)
    holdOutput = None
    holdInt = 0
    output = []
    outputCount = 0
    useHold = False
    while length >= 1:
        curStep = querySteps[length - 1] # read steps bottom up
        curLayer = int(querySteps[length - 1][0:2])
        aboveLayer = int(querySteps[length - 2][0:2])
        # print("length: "+ str(length)+" querystep: "+querySteps[length-1])

        if "Join" in curStep or "Loop" in curStep:  # for operations that need 2 inputs
#            if useHold:
#                # print(curStep[2:] + " on " + holdOutput[2:] + " and on " + output[2:])
#                outputStep = curStep[2:] + " on OUTPUT " + str(holdInt) + " and on OUTPUT " + str(
#                    outputCount - 1)
#                print(outputStep)
#               useHold = False
#            else:
#                # print(curStep[2:] + " on " + querySteps[length + 1][2:] + " and on " + output[2:])
#                outputStep = curStep[2:] + " on OUTPUT " + str(outputCount-2) + " and on OUTPUT " + str(outputCount-1)
#                print(outputStep)
#                if holdOutput is not None:
#                    # print(holdOutput[:2] + " " + curStep)
#                    if int(holdOutput[:2]) == int(curStep[:2]):
#                        # print("usehold is true")
#                        useHold = True
#            output.append(curStep[:2]+"OUTPUT " + str(outputCount) + ": "+outputStep)
#            outputCount += 1
            joinOutput = []
            i = 0
            limit = None
            for out in output: # get all output 1 level lower
                outLayer = int(out[:2])
                if outLayer>curLayer:
                    if limit is not None:
                        if outLayer<limit:
                            joinOutput=[]
                            limit = outLayer
                            joinOutput.append(out)

                        elif outLayer==limit:
                            joinOutput.append(out)

                    else:
                        limit = outLayer
                        joinOutput.append(out)

            outputStep = curStep[2:] + " on ("
            for joinOut in joinOutput:
                outNum = joinOut.split("OUTPUT ")[1][:1]
                outputStep += 'OUTPUT ' + outNum+", "
            outputStep += ")"
            output.append(curStep[:2]+"OUTPUT " + str(outputCount) + ": "+outputStep)
            outputCount += 1


        else:  # for operations that need 1 or 0 inputs
            if holdOutput is not None:
                # print(holdOutput[:2] + " " + curStep)
                if int(holdOutput[:2]) == int(curStep[:2]):
                    useHold = True
            if curLayer < aboveLayer:  # check if need to store output for far future use
                print(curStep[2:])
                output.append(curStep[:2] + "OUTPUT " + str(outputCount) + ": " + curStep[2:])
                holdOutput = curStep
                holdInt = outputCount
                outputCount += 1
            else:  # output not needed in far future
                if "Hash" in curStep:  # for operation that takes 1 input
                    if output[outputCount-1] is not None:
                        # print(curStep[2:] + " on " + output[outputCount-1])
                        # output[outputCount-1] = get previous output
                        print(curStep[2:] + " on OUTPUT " + str(outputCount - 1))
                        output.append(curStep[:2]+"OUTPUT " + str(outputCount) + ": " + curStep[2:] + " on OUTPUT " + str(outputCount - 1))
                        outputCount += 1
#                    else:  # for operation that takes 0 input
#                        print(curStep[2:])
#                        output.append( "OUTPUT " + str(outputCount) + ": " + curStep[2:])
#                        outputCount += 1
                else: # steps excluding hash
                    print(curStep[2:])
                    output.append(curStep[:2] + "OUTPUT " + str(outputCount) + ": " + curStep[2:])
                    outputCount += 1

        length -= 1

    print()
    for out in output:
        print(out[2:])