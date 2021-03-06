import re

all_from_clause_annotation = []
all_others_annotation = []
steps = []
subplans = []
pending_subplans = []
sql_keywords = ['select ', 'from ', 'where ', 'order by ', 'group by ', 'and ', 'or ']
key_operators = ['=', '>', '<', '~~', '>=', '<=', '!=', '<>']


# Initialise global variables
def init_global_vars():
    global all_from_clause_annotation
    global all_others_annotation
    global steps
    global subplans
    all_from_clause_annotation = []
    all_others_annotation = []
    steps = []
    subplans = []


# Main function to annotate JSON QEP
def annotate_json(processed_qep, input_query):
    init_global_vars()
    output_steps = []
    item_list = []

    # Initialise start node and begin recursive function to reach all nodes
    start_node = processed_qep['Plan']
    get_child_nodes(start_node)

    # Print annotation results
    print('+++++++++++++++ ALL FROM CLAUSE ANNOTATIONS ++++++++++++++++++')
    for lines in all_from_clause_annotation:
        print(lines)
    print('+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n')
    print('>>>>>>>>>>>>>>>>> ALL OTHER ANNOTATIONS >>>>>>>>>>>>>>>>>>>>>')
    for lines in all_others_annotation:
        print(lines)
    print('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n')

    # Label all steps with step number
    index = 1
    for step in steps:
        output_step = '(' + str(index) + '): \t' + step
        output_steps.append(output_step)
        index += 1

    # -------------------------------
    # Annotate sub-plans in steps
    # -------------------------------
    add_count = 0
    add_lines = []

    # Initialise lines to be added for each sub-plan
    for subplan in subplans:
        line = (subplan['start'] - 1, '---------------- Start (' + subplan['name'] + ') --------------')
        add_lines.append(line)
        line = (subplan['end'], '---------------- End   (' + subplan['name'] + ') --------------')
        add_lines.append(line)

    # Sort lines and insert into steps
    add_lines.sort()
    for add_line in add_lines:
        output_steps.insert(add_line[0] + add_count, add_line[1])
        add_count += 1

    # ----------------------------------------
    # Match Query to Annotation ('FROM' Clause)
    # ----------------------------------------
    reference_query = input_query.lower()
    global_start_loc = 0

    # Split query by 'SELECT' to get all plans in query
    input_plans = reference_query.split('select')
    num_plans = len(input_plans) - 1
    if num_plans < 1:
        print("Incorrect input query")
        return

    # loc keeps track of position of cursor in input query (location to be annotated)
    global_start_loc += 6
    plan_start_loc = global_start_loc
    plan_index = 0

    # For each plans found
    for plan in input_plans[1:]:
        # 'FROM' clause declaration are between 'FROM' and 'WHERE'
        from_split = plan.split(' from ')
        segment_start_loc = plan_start_loc + len(from_split[0]) + 6
        segment = from_split[1]
        where_split = segment.split(' where ')
        items = (where_split[0].split(', '))
        item_start_loc = segment_start_loc

        # For each item declared in 'FROM' clause
        for item in items:
            # Alias may be declared as 'customer C' or 'customer AS C'
            if len(item.split(' as ')) == 2:
                item_name = item.split(' as ')[0]
                alias = item.split(' as ')[1]
            else:
                item_name = item.split(' ')[0]
                alias = item.split(' ')[1]

            # Match query item to annotation
            if len(item_name) > 0 and len(all_from_clause_annotation) > 0:
                target_item = [i for i, v in enumerate(all_from_clause_annotation) if v[3] == alias]
                if len(target_item) > 0:
                    item_index = target_item[0]
                    item_step = all_from_clause_annotation[item_index][1]
                    item_node_type = all_from_clause_annotation[item_index][2]
                    item_tuple = (item_name, item_start_loc, item_step, item_node_type, alias)
                    item_list.append(item_tuple)
                    item_start_loc += len(item) + 2

        plan_start_loc += len(plan) + 6
        plan_index += 1

    # ---------------------------------------------
    # Insert Annotation into Query ('from' Clause)
    # ---------------------------------------------
    i = 0
    offset = 0
    item_index = 0
    annotation_output = ''
    output_query = input_query

    print('*************** ANNOTATE \'FROM\' ***************')

    # Match positions of query and annotations, and print out annotations
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
                    cursor = cur_item[1] + offset + len(cur_item[0]) + len(cur_item[4]) + 2
                    check = cur_item[1] + offset + len(cur_item[0]) + 1
                    if output_query[check:check + 2].lower() == 'as':
                        cursor += 3
                    print(cur_item)
                    output_query = output_query[:cursor].ljust(diff + len(output_query[:cursor]), ' ') + output_query[
                                                                                                         cursor:]
                    offset += diff
                    i -= diff
            else:
                annotation_output += ' '
                i += 1
        else:
            annotation_output += ' '
            i += 1

    print('***********************************************\n')

    # ----------------------------------------------------------------------------
    # Format query (Split query and annotations line-by-line using sql keywords)
    # ----------------------------------------------------------------------------
    output_query_reference = output_query.lower()
    split_pos_list = []

    # Find all split points
    for keyword in sql_keywords:
        cur = 0
        while output_query_reference.find(keyword, cur) != -1:
            index = output_query_reference.find(keyword, cur)
            split_pos_list.append(index)
            cur = index + len(keyword)
    split_pos_list.sort()

    # Split query and annotation using sorted split points
    cur_pos = split_pos_list[0]
    query_list = []
    annotation_list = []
    for split_pos in split_pos_list[1:]:
        query_list.append(output_query[cur_pos:split_pos])
        annotation_list.append(annotation_output[cur_pos:split_pos])
        cur_pos = split_pos
    query_list.append(output_query[cur_pos:])
    annotation_list.append(annotation_output[cur_pos:])

    # ----------------------------
    # Annotate Query (Others)
    # ----------------------------
    print('###################### ANNOTATE OTHERS ##################')
    keywords = ['where', 'and', 'or']
    line_num = 0

    # Annotate input query line-by-line
    for query_line in query_list:
        words = query_line.lower().strip().split(' ')
        operand_tuples = []

        # If line contains 'WHERE', 'OR', 'AND', line will contain an expression
        if words[0] in keywords:
            # Remove unnecessary brackets
            operands = [re.sub('[()]', '', words[1]), re.sub('[()]', '', words[3])]
            for operand in operands:
                # Split alias and name, if present.
                # Else, it may be either a sub-plan, numerical constant, or string.
                elements = operand.split('.')
                if len(elements) > 1:
                    if len(re.findall("^[0-9]*$", elements[0])) > 0 and len(re.findall("^[0-9]*$", elements[1])) > 0:
                        alias = '$NUM'
                        name = operand
                    else:
                        alias = elements[0]
                        name = elements[1]
                else:
                    name = elements[0]
                    if name == '':
                        alias = '$SP'
                        name = 'SubPlan'
                    elif len(re.findall("^[0-9]*$", name)) > 0:
                        alias = '$NUM'
                    elif len(re.findall("[\'\"]", name)) > 0:
                        alias = '$STR'
                        name = name.strip('\'')
                    else:
                        print('ERROR')
                        alias = 'ERR'
                operand_tuples.append((alias, name))

            # Convert equivalent operators
            operator = words[2]
            if operator == 'like':
                operator = '~~'
            if operator == '!=':
                operator = '<>'

            print((operator, operand_tuples))

            # Filter list by operator
            filtered = list(filter(lambda x: (x[0] == operator), all_others_annotation))

            # Attempt to match query expression to annotated list
            match = None
            for x in filtered:
                is_match = True
                for e in x[1]:
                    if e[1][0] == '$' or e[1] == 'SubPlan':
                        if (operand_tuples[0][0] != e[0]) and (operand_tuples[1][0] != e[0]):
                            is_match = False
                    else:
                        if (operand_tuples[0] != e) and (operand_tuples[1] != e):
                            is_match = False
                if is_match:
                    match = x
            print('matched: ', match)

            # Add annotation to annotation list
            if match is not None:
                spacing = ''.ljust(len(words[0]) + 1)
                annotation_list[line_num] = spacing + '(' + str(match[2]) + ')' + match[3]

        line_num += 1
    print('########################################################\n')

    # --------------------
    # Generate Output
    # --------------------
    annotated_query = []
    for (query_line, annotation_line) in zip(query_list, annotation_list):
        annotated_query.append(query_line)
        annotated_query.append(annotation_line)

    return output_steps, annotated_query


# Recursive function to cover all nodes in the QEP
def get_child_nodes(cur_node):
    global all_from_clause_annotation
    global all_others_annotation

    # If there are no child plans in node (leaf node)
    # Only can occur with table reading ('from' clause)
    if 'Plans' not in cur_node:

        # Build step and append to list
        relation_name = cur_node['Relation Name']
        acting_on = '[' + relation_name + ']'
        step = build_step(cur_node, acting_on)
        steps.append(step)
        step_index = len(steps)

        # If there are pending sub-plans to be located, current step is the start location
        # All plans always start with reading a table
        if len(pending_subplans) > 0:
            subplan_index = pending_subplans.pop()
            subplans[subplan_index]['start'] = step_index

        # Add annotation of 'from' clause to list
        all_from_clause_annotation.append((relation_name, step_index, cur_node['Node Type'], cur_node['Alias']))

    # All other nodes
    else:
        child_plans = cur_node["Plans"]
        child_index = []

        # Indicates a new sub-plan is present
        # Adds sub-plan to pending sub-plans to be located
        if 'Subplan Name' in cur_node:
            subplan = {'name': cur_node['Subplan Name'], 'start': '', 'end': ''}
            subplans.append(subplan)
            subplan_index = len(subplans) - 1
            pending_subplans.append(subplan_index)

        # For all child nodes of current node, process child nodes recursively
        for child_plan in child_plans:
            # For each child node processed, gather index of plans corresponding to child nodes
            child_index.append(get_child_nodes(child_plan))
        acting_on = ''

        # Step of current node acts on all its child nodes
        for index in child_index:
            acting_on += '(' + str(index) + ')'
        step = build_step(cur_node, acting_on)
        steps.append(step)
        step_index = len(steps)

        # After iterating all its child nodes, the last step is the end of the current sub-plan
        if 'Subplan Name' in cur_node:
            subplans[subplan_index]['end'] = step_index

    # Get other annotations
    build_other_annotation(cur_node, step_index)

    return step_index


# Build natural language step
def build_step(node, acting_on):
    step = ''
    step += node['Node Type'] + ' '
    step += 'is performed on '
    step += acting_on
    step += ' '
    if 'Merge Cond' in node:
        step += 'with merge condition ' + node['Merge Cond'] + ' '
    if 'Hash Cond' in node:
        step += 'with hash condition ' + node['Hash Cond'] + ' '
    if 'Index Cond' in node:
        step += 'with index condition ' + node['Index Cond'] + ' '
    if 'Filter' in node:
        step += 'with filter ' + node['Filter'] + ' '
    if 'Join Filter' in node:
        step += 'with join filter ' + node['Join Filter'] + ' '
    if 'Cache Key' in node:
        step += 'with cache key (' + node['Cache Key'] + ') '
    if 'Subplan Name' in node:
        step += 'to form (' + node['Subplan Name'] + ') '
    return step


# Build other annotation from node
def build_other_annotation(node, step_index):
    annotations = []

    # Determine type of annotation and append to list
    label = node['Node Type']
    if 'Merge Cond' in node:
        annotations.append((node['Merge Cond'], label))
    if 'Hash Cond' in node:
        annotations.append((node['Hash Cond'], label))
    if 'Index Cond' in node:
        label = 'Index Cond'
        annotations.append((node['Index Cond'], label))
    if 'Filter' in node:
        label = 'Filter'
        annotations.append((node['Filter'], label))
    if 'Join Filter' in node:
        label = 'Join Filter'
        annotations.append((node['Join Filter'], label))

    # For all annotations found in the node
    for annotation, label in annotations:
        # Remove brackets
        annotation = re.sub('[()]', '', annotation)
        # May contain more than 1 expressions joined by 'AND' or 'OR'
        expressions = re.split(r' AND | OR ', annotation)

        for expression in expressions:
            elements = expression.split(' ')
            # If expression contain sub-plans, remove addition element caused by extra spacing
            if 'SubPlan' in elements:
                index = elements.index('SubPlan')
                del elements[index + 1]

            # 1st and 3rd elements are the operands, 2nd element is the operator
            operator = elements[1]
            operands = []
            for element in [elements[0], elements[2]]:
                # Split element by alias and name, if present
                # Else get alias from node
                # If name starts with '$' or no alias in node, it is a sub-plan
                split = element.split('.')
                if len(split) == 2:
                    if len(re.findall("^[0-9]*$", split[0])) > 0 and len(re.findall("^[0-9]*$", split[1])) > 0 :
                        alias = '$NUM'
                        name = element
                    else:
                        alias = split[0]
                        name = split[1]
                elif 'Alias' in node:
                    alias = node['Alias']
                    name = split[0]
                    if split[0][0] == '$':
                        alias = '$SP'
                else:
                    alias = '$SP'
                    name = split[0]

                # Remove unnecessary for constants
                # Define alias representing constants
                print(name)
                temp = name.split('::')
                name = temp[0]
                if len(re.findall("^[0-9]*$", name)) > 0:
                    alias = '$NUM'
                if temp[0][0] == '\'' and temp[0][-1] == '\'':
                    name = temp[0].strip('\'')
                    if len(re.findall("^[0-9]*$", name)) > 0:
                        alias = '$NUM'
                    else:
                        alias = '$STR'
                        name = name.lower()
                operands.append((alias, name))

            all_others_annotation.append((operator, operands, step_index, label))
