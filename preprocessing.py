import re


def preprocessing(qep):
    count = 0
    filteredSteps = []
    for step in qep:
        plan = step[0]
        # remove unnecessary brackets
        # print("count= "+str(count))
        # print(plan)
        if '->' in plan:
            plan = re.sub("[(\[].*?[\)\]]", "", plan)
            plan = plan.replace("->", "")
            layer = len(plan) - len(plan.lstrip(' '))
            strLayer = str(layer)
            if layer >= 0 and layer <= 9:
                strLayer = "0" + strLayer
            plan = strLayer + " " + plan.strip()

            filteredSteps.append(plan)
            count += 1
        else:
            if count > 0 and "Planning" not in plan and "Execution" not in plan:
                filteredSteps[count - 1] += " with " + plan.strip()
            else:
                plan = re.sub("[(\[].*?[\)\]]", "", plan)
                layer = len(plan) - len(plan.lstrip(' '))
                strLayer = str(layer)
                if layer >= 0 and layer <= 9:
                    strLayer = "0" + strLayer
                plan = strLayer + " " + plan.strip()

                filteredSteps.append(plan)
                count += 1

    print('steps')
    for step in filteredSteps:
        print(step)
    print()

    return filteredSteps