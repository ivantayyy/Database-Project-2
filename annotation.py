import json
import re


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
cur_others_annotation = []
all_others_annotation = []
steps = []
subplans = []
pending_subplans = []
sql_keywords = ['select ', 'from ', 'where ', 'order by ', 'group by ', 'and ', 'or ']
key_operators = ['=', '>', '<', '~~', '>=', '<=', '!=', '<>']


def annotate_json(processed_qep, input_query):
    output_steps = []
    item_list = []
    plan = processed_qep['Plan']

    print()
    cur_plan = plan
    get_child_plans(cur_plan)
    # all_from_clause_annotation.append(cur_from_clause_annotation)
    print('\nALL OTHER ANNOTATIONS >>>>>>>>>>>>>>>>>>>>>')
    for lines in all_others_annotation:
        print(lines)
    print('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n')

    index = 1
    for step in steps:
        output_step = '(' + str(index) + '): \t' + step
        # print(output_step)
        output_steps.append(output_step)
        index += 1

    add_count = 0
    add_lines = []
    for subplan in subplans:
        print(subplan)
        line = (subplan['start'] - 1, '---------------- Start (' + subplan['name'] + ') --------------')
        add_lines.append(line)
        line = (subplan['end'], '---------------- End   (' + subplan['name'] + ') --------------')
        add_lines.append(line)
    print(add_lines)
    add_lines.sort()
    print(add_lines)
    for add_line in add_lines:
        output_steps.insert(add_line[0] + add_count, add_line[1])
        add_count += 1

    # print(len(all_from_clause_annotation))
    print("all_from_clause_annotation: ")
    print(all_from_clause_annotation)
    print()

    # --------------------
    # Annotate Query (From)
    # --------------------

    reference_query = input_query.lower()

    global_start_loc = 0
    input_plans = reference_query.split('select')
    # print(len(input_plans))
    # for lines in input_plans:
    #     print(lines)
    num_plans = len(input_plans) - 1
    if num_plans < 1:
        print("Incorrect input query")
        return
    global_start_loc += 6
    plan_start_loc = global_start_loc
    plan_index = 0
    for plan in input_plans[1:]:
        from_split = plan.split(' from ')
        # print('plan_start_loc: ', plan_start_loc)
        # print(from_split)
        segment_start_loc = plan_start_loc + len(from_split[0]) + 6
        segment = from_split[1]

        where_split = segment.split(' where ')
        # print(where_split[0])
        items = (where_split[0].split(', '))
        item_start_loc = segment_start_loc
        for item in items:
            # print('item_start_loc', item_start_loc)
            if len(item.split(' as ')) == 2:
                item_name = item.split(' as ')[0]
                alias = item.split(' as ')[1]
            else:
                item_name = item.split(' ')[0]
                alias = item.split(' ')[1]
            # print('plan_index: ', plan_index)
            if len(item_name) > 0 and len(all_from_clause_annotation) > 0:
                # print('Item name:')
                # print(item_name)
                # print(alias)
                # print(all_from_clause_annotation)
                target_item = [i for i, v in enumerate(all_from_clause_annotation) if v[3] == alias]
                if len(target_item) > 0:
                    # print("target_item: ", target_item)
                    item_index = target_item[0]
                    item_step = all_from_clause_annotation[item_index][1]
                    item_node_type = all_from_clause_annotation[item_index][2]
                    # print(item_step)
                    item_tuple = (item_name, item_start_loc, item_step, item_node_type, alias)
                    item_list.append(item_tuple)
                    # print('here')
                    # print(item_tuple)
                    item_start_loc += len(item) + 2

        plan_start_loc += len(plan) + 6
        plan_index += 1

    # --------------------
    # Insert Annotation
    # --------------------
    i = 0
    offset = 0
    item_index = 0
    annotation_output = ''
    output_query = input_query
    # print(item_list[-1][1])
    while i < len(reference_query):
        if item_index < len(item_list):
            cur_item = item_list[item_index]
            if cur_item[1] == i:
                step_str = '(' + str(cur_item[2]) + ')' + cur_item[3] + ' '
                annotation_output += step_str
                item_index += 1
                i += len(step_str)
                diff = len(step_str) - (len(cur_item[0]) + len(cur_item[4]) + 2)
                if diff > 0:
                    # print(diff)
                    cursor = cur_item[1] + offset + len(cur_item[0]) + len(cur_item[4]) + 2
                    print(cur_item)
                    # cursor = cur_item[1] + offset
                    output_query = output_query[:cursor].ljust(diff + len(output_query[:cursor]), ' ') + output_query[
                                                                                                         cursor:]
                    offset += diff
                    i -= diff
                # print('---')
                # print('item: ', cur_item[1])
                # print('i: ', i)
                # print(output_query)
                # print(annotation_output)
            else:
                annotation_output += ' '
                i += 1
        else:
            annotation_output += ' '
            i += 1

    # print('\nAnnotated Query:')
    # print(output_query)
    # print(annotation_output)

    # --------------------
    # Format query
    # --------------------
    output_query_reference = output_query.lower()
    split_pos_list = []
    for keyword in sql_keywords:
        cur = 0
        while output_query_reference.find(keyword, cur) != -1:
            index = output_query_reference.find(keyword, cur)
            split_pos_list.append(index)
            cur = index + len(keyword)
    split_pos_list.sort()
    # print(split_pos_list)

    cur_pos = split_pos_list[0]
    query_list = []
    annotation_list = []
    # print(split_pos_list)
    # print(output_query)
    # print(annotation_output)
    for split_pos in split_pos_list[1:]:
        query_list.append(output_query[cur_pos:split_pos])
        annotation_list.append(annotation_output[cur_pos:split_pos])
        cur_pos = split_pos
    query_list.append(output_query[cur_pos:])
    annotation_list.append(annotation_output[cur_pos:])

    # --------------------
    # Annotate Query (Others)
    # --------------------
    print('\n###################### ANNOTATE OTHERS ##################')
    keywords = ['where', 'and', 'or']
    line_num = 0
    for query_line in query_list:
        words = query_line.lower().strip().split(' ')
        # print(words)
        # expression = ''.join(words[1:])
        operand_tuples = []
        if words[0] in keywords:
            operands = [re.sub('[()]', '', words[1]), re.sub('[()]', '', words[3])]
            # print(operands)
            for operand in operands:
                elements = operand.split('.')
                if len(elements) > 1:
                    operand_tuples.append((elements[0], elements[1]))
                else:
                    name = elements[0]
                    if name == '':
                        # print('subplan')
                        alias = '$SP'
                        name = 'SubPlan'
                    elif len(re.findall("^[0-9]*$", name)) > 0:
                        # print('numerical')
                        alias = '$NUM'
                    elif len(re.findall("[\'\"]", name)) > 0:
                        # print('string')
                        alias = '$STR'
                    else:
                        print('ERROR')
                        alias = 'ERR'
                    operand_tuples.append((alias, name))
            operator = words[2]
            if operator == 'like':
                operator = '~~'
            if operator == '!=':
                operator = '<>'
            print((operator, operand_tuples))

            # Matching
            filtered = list(filter(lambda x: (x[0] == operator), all_others_annotation))
            match = None
            for x in filtered:
                for e in x[1]:
                    if operand_tuples[0][0] == e[0]:
                        match = x
            print('matched: ', match)
            if match is not None:
                spacing = ''.ljust(len(words[0]) + 1)
                annotation_list[line_num] = spacing + '(' + str(match[2]) + ')' + match[3]
            print()
        line_num += 1
    print('########################################################\n')

    annotated_query = []
    # print(query_list),
    # print(annotation_list)
    for (query_line, annotation_line) in zip(query_list, annotation_list):
        annotated_query.append(query_line)
        annotated_query.append(annotation_line)

    return output_steps, annotated_query


def get_child_plans(cur_plan):
    global cur_from_clause_annotation
    global all_from_clause_annotation
    global cur_others_annotation
    global all_others_annotation

    step_index = 0

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
            # all_from_clause_annotation.append(cur_from_clause_annotation)
            # cur_from_clause_annotation = []
            # all_others_annotation.append(cur_from_clause_annotation)
            # cur_others_annotation = []

        all_from_clause_annotation.append((relation_name, step_index, cur_plan['Node Type'], cur_plan['Alias']))

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
        step_index = len(steps)
        if 'Subplan Name' in cur_plan:
            subplans[subplan_index]['end'] = step_index

    # Get other annotations
    build_other_annotation(cur_plan, step_index)

    return step_index


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


def build_other_annotation(cur_plan, step_index):
    annotations = []

    if 'Merge Cond' in cur_plan:
        annotations.append(cur_plan['Merge Cond'])
    if 'Hash Cond' in cur_plan:
        annotations.append(cur_plan['Hash Cond'])
    if 'Index Cond' in cur_plan:
        annotations.append(cur_plan['Index Cond'])
    if 'Filter' in cur_plan:
        annotations.append(cur_plan['Filter'])
    if 'Join Filter' in cur_plan:
        annotations.append(cur_plan['Join Filter'])

    for annotation in annotations:
        annotation = re.sub('[()]', '', annotation)
        expressions = re.split(r' AND | OR ', annotation)
        # print("annotation: ", annotation)
        # print("elements: ", elements)
        for expression in expressions:
            elements = expression.split(' ')
            if 'SubPlan' in elements:
                index = elements.index('SubPlan')
                # elements[index] = ' '.join([elements[index], elements[index + 1]])
                del elements[index + 1]
            operator = elements[1]
            operands = []
            for element in [elements[0], elements[2]]:
                split = element.split('.')
                if len(split) == 2:
                    alias = split[0]
                    name = split[1]
                elif 'Alias' in cur_plan:
                    alias = cur_plan['Alias']
                    name = split[0]
                    print(cur_plan)
                    if split[0][0] == '$':
                        alias = '$SP'
                else:
                    alias = '$SP'
                    name = split[0]
                    print(cur_plan)
                name = name.split('::')[0]
                if len(re.findall("^[0-9]*$", name)) > 0:
                    print('numerical')
                    alias = '$NUM'
                if len(re.findall("[\'\"]", name)) > 0:
                    print('string')
                    alias = '$STR'
                    name = name.lower()
                operands.append((alias, name))

            all_others_annotation.append((operator, operands, step_index, cur_plan['Node Type']))
