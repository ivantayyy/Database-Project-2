import json


def annotate(query_steps):
    annotate_from = []
    length = len(query_steps)
    hold_output = None
    hold_int = 0
    output = []
    output_count = 0
    use_hold = False
    while length >= 1:
        cur_step = query_steps[length - 1]  # read steps bottom up
        cur_layer = int(query_steps[length - 1][0:2])
        above_layer = int(query_steps[length - 2][0:2])
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
                if out_layer > cur_layer:
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
            output_step = output_step[:len(output_step) - 2]
            output_step += ")"
            output.append(cur_step[:2] + "OUTPUT " + str(output_count) + ": " + output_step)
            output_count += 1


        else:  # for operations that need 1 or 0 inputs
            if hold_output is not None:
                # print(hold_output[:2] + " " + cur_step)
                if int(hold_output[:2]) == int(cur_step[:2]):
                    use_hold = True
            if cur_layer < above_layer:  # check if need to store output for far future use
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
                    split = str.split(cur_step[3:], ' on ')
                    if len(split) == 2:
                        annotate_from.append((split[1], split[0]))
                    output_count += 1

        length -= 1

    print()
    final_output = []
    for out in output:
        print(out[2:])
        final_output.append(out[2:])

    print(final_output)
    print(annotate_from)
    return final_output


cur_from_clause_annotation = []
all_from_clause_annotation = []
steps = []
subplans = []
pending_subplans = []


def annotate_json(processed_qep, input_query):
    annotation = []
    plan = processed_qep['Plan']

    print()
    cur_plan = plan
    get_child_plans(cur_plan)
    all_from_clause_annotation.append(cur_from_clause_annotation)

    index = 1
    for step in steps:
        print('(' + str(index) + '): ' + step)
        index += 1

    for subplan in subplans:
        print(subplan)

    index = 0
    # print(len(all_from_clause_annotation))
    # print(all_from_clause_annotation)
    for plan_annotation in all_from_clause_annotation:
        print('Plan ' + str(index) + ':')
        for annotation in plan_annotation:
            print(annotation)
        index += 1

    return annotation


def get_child_plans(cur_plan):
    global cur_from_clause_annotation
    global all_from_clause_annotation

    if 'Plans' not in cur_plan:
        # print('No child plans')
        relation_name = cur_plan['Relation Name']
        acting_on = '[' + relation_name + ']'
        step = build_step(cur_plan, acting_on)
        steps.append(step)
        step_index = len(steps)

        if len(pending_subplans) > 0:
            subplan_index = pending_subplans.pop()
            subplans[subplan_index]['start'] = step_index
            all_from_clause_annotation.append(cur_from_clause_annotation)
            cur_from_clause_annotation = []

        cur_from_clause_annotation.append((relation_name, step_index))

        return step_index
    else:
        child_plans = cur_plan["Plans"]
        child_index = []

        if 'Subplan Name' in cur_plan:
            subplan = {'name': cur_plan['Subplan Name'], 'start': '', 'end': ''}
            subplans.append(subplan)
            subplan_index = len(subplans) - 1
            pending_subplans.append(subplan_index)

        # print(cur_plan['Node Type'] + ' : [')
        for child_plan in child_plans:
            child_index.append(get_child_plans(child_plan))
        acting_on = ''
        for index in child_index:
            acting_on += '(' + str(index) + ')'
        step = build_step(cur_plan, acting_on)

        steps.append(step)
        if 'Subplan Name' in cur_plan:
            subplans[subplan_index]['end'] = len(steps)
        return len(steps)


def build_step(cur_plan, acting_on):
    step = ''
    step += cur_plan['Node Type'] + ' '
    # if 'Join Type' in cur_plan:
    #     step += '(' + cur_plan['Join Type'] + ') '
    step += 'is performed on '
    step += acting_on
    step += ' '
    if 'Merge Cond' in cur_plan:
        step += 'with merge condition ' + cur_plan['Merge Cond'] + ' '
    if 'Hash Cond' in cur_plan:
        step += 'with hash condition ' + cur_plan['Hash Cond'] + ' '
    if 'Index Cond' in cur_plan:
        step += 'with index condition ' + cur_plan['Index Cond'] + ' '
    if 'Filter' in cur_plan:
        step += 'with filter ' + cur_plan['Filter'] + ' '
    if 'Join Filter' in cur_plan:
        step += 'with join filter ' + cur_plan['Join Filter'] + ' '
    if 'Cache Key' in cur_plan:
        step += 'with cache key (' + cur_plan['Cache Key'] + ') '
    if 'Subplan Name' in cur_plan:
        step += 'to form (' + cur_plan['Subplan Name'] + ') '
    return step
