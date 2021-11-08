def annotate(query_steps):
    output = []
    length = len(query_steps)
    hold_output = None
    hold_int = 0
    output = []
    output_count = 0
    use_hold = False
    while length >= 1:
        cur_step = query_steps[length - 1]  # read steps bottom up
        curLayer = int(query_steps[length - 1][0:2])
        aboveLayer = int(query_steps[length - 2][0:2])
        # print("length: "+ str(length)+" querystep: "+query_steps[length-1])

        if "Join" in cur_step or "Loop" in cur_step:  # for operations that need 2 inputs
            #            if use_hold:
            #                # print(cur_step[2:] + " on " + hold_output[2:] + " and on " + output[2:])
            #                output_step = cur_step[2:] + " on OUTPUT " + str(hold_int) + " and on OUTPUT " + str(
            #                    output_count - 1)
            #                print(output_step)
            #               use_hold = False
            #            else:
            #                # print(cur_step[2:] + " on " + querySteps[length + 1][2:] + " and on " + output[2:])
            #                output_step = cur_step[2:] + " on OUTPUT " + str(output_count-2) + " and on OUTPUT " + str(output_count-1)
            #                print(output_step)
            #                if hold_output is not None:
            #                    # print(hold_output[:2] + " " + cur_step)
            #                    if int(hold_output[:2]) == int(cur_step[:2]):
            #                        # print("usehold is true")
            #                        use_hold = True
            #            output.append(cur_step[:2]+"OUTPUT " + str(output_count) + ": "+output_step)
            #            output_count += 1
            join_output = []
            i = 0
            limit = None
            for out in output:  # get all output 1 level lower
                out_layer = int(out[:2])
                if out_layer > curLayer:
                    if limit is not None:
                        if out_layer < limit:
                            join_output = []
                            limit = out_layer
                            join_output.append(out)

                        elif out_layer == limit:
                            join_output.append(out)

                    else:
                        limit = out_layer
                        join_output.append(out)

            output_step = cur_step[3:] + " on ("
            for joinOut in join_output:
                out_num = joinOut.split("OUTPUT ")[1][:1]
                output_step += 'OUTPUT ' + out_num + ", "
            output_step = output_step[:len(output_step)-2]
            output_step += ")"
            output.append(cur_step[:2] + "OUTPUT " + str(output_count) + ": " + output_step)
            output_count += 1

        else:  # for operations that need 1 or 0 inputs
            if hold_output is not None:
                # print(hold_output[:2] + " " + cur_step)
                if int(hold_output[:2]) == int(cur_step[:2]):
                    use_hold = True
            if curLayer < aboveLayer:  # check if need to store output for far future use
                print(cur_step[3:])
                output.append(cur_step[:2] + "OUTPUT " + str(output_count) + ": " + cur_step[3:])
                hold_output = cur_step
                hold_int = output_count
                output_count += 1
            else:  # output not needed in far future
                if "Hash" in cur_step:  # for operation that takes 1 input
                    if output[output_count - 1] is not None:
                        # print(cur_step[2:] + " on " + output[output_count-1])
                        # output[output_count-1] = get previous output
                        print(cur_step[3:] + " on OUTPUT " + str(output_count - 1))
                        output.append(
                            cur_step[:2] + "OUTPUT " + str(output_count) + ": " + cur_step[3:] + " on OUTPUT " + str(
                                output_count - 1))
                        output_count += 1
                #                    else:  # for operation that takes 0 input
                #                        print(cur_step[2:])
                #                        output.append( "OUTPUT " + str(output_count) + ": " + cur_step[2:])
                #                        output_count += 1
                else:  # steps excluding hash
                    print(cur_step[3:])
                    output.append(cur_step[:2] + "OUTPUT " + str(output_count) + ": " + cur_step[3:])
                    output_count += 1

        length -= 1

    print()
    final_output = []
    for out in output:
        print(out[2:])
        final_output.append(out[2:])

    print(final_output)
    return final_output
