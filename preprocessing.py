import re
import json


def preprocessing(qep):
    count = 0
    filtered_steps = []
    for step in qep:
        plan = step[0]
        # remove unnecessary brackets
        # print("count= "+str(count))
        # print(plan)
        if '->' in plan:
            plan = re.sub("[(\[].*?[\)\]]", "", plan)
            plan = plan.replace("->", "")
            layer = len(plan) - len(plan.lstrip(' '))
            str_layer = str(layer)
            if layer >= 0 and layer <= 9:
                str_layer = "0" + str_layer
            plan = str_layer + " " + plan.strip()

            filtered_steps.append(plan)
            count += 1
        else:
            if count > 0 and "Planning" not in plan and "Execution" not in plan:
                filtered_steps[count - 1] += " with " + plan.strip()
            else:
                plan = re.sub("[(\[].*?[\)\]]", "", plan)
                layer = len(plan) - len(plan.lstrip(' '))
                str_layer = str(layer)
                if layer >= 0 and layer <= 9:
                    str_layer = "0" + str_layer
                plan = str_layer + " " + plan.strip()

                filtered_steps.append(plan)
                count += 1

    print('steps')
    for step in filtered_steps:
        print(step)
    print()

    return filtered_steps


def preprocess_json(qep):
    qep = qep[0]
    qep = qep[0]
    qep = qep[0]
    print('Pre-processed QEP: ')
    print(type(qep))
    print(qep)
    # print(qep['Plan']['Node Type'])
    # print(qep[4:len(qep)-5])
    # qep_dict = json.loads(qep)
    # print(qep_dict['Plan'])

    return qep
