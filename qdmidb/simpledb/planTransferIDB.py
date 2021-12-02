DATASET = 'CDC'

path = "/Users/yiminglin/Documents/Codebase/QDMIDB/QDMIDB/qdmidb/queryplancodes/cdc/cdcQueryPlans.txt"
output_path = '/Users/yiminglin/Documents/Codebase/QDMIDB/QDMIDB/qdmidb/queryplancodes/cdc/querycodeIDB.txt'


f = open(path).readlines()

order_list = []
tree_dict = {}
temp_node =[] # in case of | node
need_father=[]


for line in f:
    #node
    node = line.split(':')[0]
    #child
    child = line.split(':')[1]
    if "|" not in node:
        need_father.append(node)
        order_list.append(node)
        tree_dict[node] = []
        if len(child)>1: #not leave node
            child_nodes = child.split(']')
            if len(child_nodes)>2: #in case of join len=3
                # c1= child_nodes[0].split('[')[1]
                # c2= child_nodes[1].split('[')[1]

                tree_dict[node].append(need_father[0])
                tree_dict[node].append(need_father[1])
                need_father.pop(0)
                need_father.pop(0)

            else:
                c= child_nodes[0].split('[')[1]
                if "|" not in c:
                    idx = need_father.index(c)
                    need_father.pop(idx)
                    tree_dict[node].append(c)
                else:
                    tree_dict[node].append(need_father[-2])
                    need_father.pop(-2) #pop the last one


print("order: ")
print(order_list)
print("dict: ")
print(tree_dict)
#rewrite impute relationship
new_order_list = []
for idx in range(len(order_list)):
    node = order_list[idx]
    if ('μ' in node) and (',' in node):
        new_node = []
        parent_node = order_list[idx+1]
        arg_list = node.split('(')[1].split(')')[0]
        for i in arg_list.split(','):
            new_node.append('μ('+i+')')
            new_order_list.append('μ('+i+')')
        #change impute and its child nodes relationship
        for j in range(len(new_node)):
            if node in tree_dict:
                tree_dict[new_node[j]] = tree_dict[node]
                tree_dict.pop(node,None)
            else:
                tree_dict[new_node[j]] = [new_node[j-1]]
        #change impute's parents relationship
        tree_dict[parent_node] = [new_node[-1]]
    else:
        new_order_list.append(node)
print("after:")
print(new_order_list)
print(tree_dict)

## create node name and save in a dictionary
name_dict = {} #'original name ': nikname
scanNum=1
imputeNum=1
selNum =1
joinNum =1
aggNum=1
for node in new_order_list:
    NodeName =''
    if 'scan(' in node:
        scanVal = node.split("(")[1].split(")")[0]
        NodeName='s'+str(scanNum)+scanVal
        scanNum += 1
    if 'μ' in node:
        imputeVal = node.split("(")[1].split(")")[0]
        NodeName = 'imp'+str(imputeNum)+imputeVal
        imputeNum += 1
    if 'σ' in node:
        selVal = node.split("(")[1].split(")")[0]
        NodeName = 'sel' + str(selNum) + selVal
        selNum += 1
    if  '⨝' in node:
        joinVal = node.split("(")[1].split(")")[0]
        NodeName = 'join' + str(joinNum) + joinVal
        joinNum += 1
    if 'g(' in node or 'min(' in node or 'max(' in node or 'avg(' in node or 'sum(' in node or 'count(' in node:
        aggVal = node.split("(")[1].split(")")[0]
        NodeName = 'agg' + str(aggNum) + aggVal
        aggNum += 1
    if 'π' in node:
        NodeName = 'sp'
    if '.' in NodeName:
        NodeName = NodeName.split('.')[0]
    name_dict[node] = NodeName

print(name_dict)




output = ''
preNameNum=1
piNameNum = 1
attr_pi = []
groupChildName = ''
aggregateAttr = ''
groupAttr = ''
aggOp = ''
for node in new_order_list:
    if 'scan' in node:
        scanVal = node.split("(")[1].split(")")[0]
        scanNodeName = name_dict[node]
        tableName = DATASET + scanVal
        output += 'SeqScan '+ scanNodeName + '= new SeqScan(tid, ' + tableName + ".getId(), " + "\"" + scanVal + "\");\n"

    if 'μ' in node:
        imputeVal = node.split("(")[1].split(")")[0]
        imputeNodeName = name_dict[node]
        childNodeName = name_dict[tree_dict[node][0]]
        output += 'Impute '+imputeNodeName+' = new Impute(new Attribute(\"' + imputeVal + "\"),"+childNodeName+");\n"

    if 'σ' in node:
        selVal = node.split("(")[1].split(")")[0]
        selNodeName = name_dict[node]
        op=''
        oprand=''
        selVal1 = ''
        if '=' in selVal and '>=' not in selVal and '<=' not in selVal:
            op = 'EQUALS'
            oprand = selVal.split('=')[1]
            selVal1=selVal.split('=')[0]
        elif '>' in selVal and '>=' not in selVal:
            op = 'GREATER_THAN'
            oprand = selVal.split('>')[1]
            selVal1 = selVal.split('>')[0]
        elif '>=' in selVal:
            op = 'GREATER_THAN_OR_EQ'
            oprand = selVal.split('>=')[1]
            selVal1 = selVal.split('>=')[0]
        elif '<' in selVal and '<=' not in selVal:
            op = 'LESS_THAN'
            oprand = selVal.split('<')[1]
            selVal1 = selVal.split('<')[0]
        elif '<=' in selVal:
            op = 'LESS_THAN_OR_EQ'
            oprand = selVal.split('<=')[1]
            selVal1 = selVal.split('<=')[0]
        childNodeName = name_dict[tree_dict[node][0]]
        output+='SmartFilter '+selNodeName+' = new SmartFilter(new Predicate(\"'+selVal1+ '\", Predicate.Op.'+op+', new IntField('+oprand+')), '+childNodeName +');\n'

    if '⨝' in node:
        joinVal = node.split("(")[1].split(")")[0]
        arg1 = joinVal.split('=')[0]
        arg2 = joinVal.split('=')[1]
        joinNodeName = name_dict[node]
        child1Name = name_dict[tree_dict[node][0]]
        child2Name = name_dict[tree_dict[node][1]]
        predicateName = 'predName' + str(preNameNum)
        preNameNum +=1
        output += 'JoinPredicate '+predicateName + ' = new JoinPredicate(\"' + arg1+'\", Predicate.Op.EQUALS, \"' +arg2+'\");\n'
        output += 'SmartJoin ' +joinNodeName + ' = new SmartJoin('+predicateName+', '+child1Name+', '+child2Name+");\n"

    if 'g(' in node or 'min(' in node or 'max(' in node or 'avg(' in node or 'sum(' in node or 'count(' in node:
        if ',' in node:
            nodelist = node.split(',')
        else:
            nodelist = [node]
        groupNodeName = name_dict[node]
        groupChildName = name_dict[tree_dict[node][0]]

        for n in nodelist:
            aggVal = n.split("(")[1].split(")")[0]
            if aggVal not in attr_pi:
                attr_pi.append(aggVal)
            aggIndicator = n.split('(')[0]
            if aggIndicator == 'g':
                groupAttr = aggVal
            else:
                aggregateAttr = aggVal
                if  'min' in aggIndicator:
                    aggOp = 'MIN'
                elif  'max' in aggIndicator :
                    aggOp = 'MAX'
                elif  'sum' in aggIndicator :
                    aggOp = 'SUM'
                elif 'avg' in aggIndicator :
                    aggOp = 'AVG'
                elif 'count' in aggIndicator :
                    aggOp = 'COUNT'

    if 'π' in node:
        piVal = node.split("(")[1].split(")")[0]
        if ',' in piVal:
            varlist = piVal.split(',')
        else:
            varlist = [piVal]
        for var in varlist:
            if 'null' not in var:
                if var not in attr_pi:
                    attr_pi.append(var)
        SmartProjectName = 'sp' + str(piNameNum)
        piNameNum += 1
        piNodeName = name_dict[node]

        output +='List<Attribute> attributes = new ArrayList<>();\n'
        for var in attr_pi:
            output +='attributes.add(new Attribute(\"'+var +'\"));\n'

        output += 'Type[] types = new Type[]{'
        for i in range(len(attr_pi)):
            if i != len(attr_pi)-1:
                output += 'Type.INT_TYPE,'
            else:
                output += 'Type.INT_TYPE};\n'

        output += 'SmartProject '+ SmartProjectName + ' = new SmartProject(attributes, types, '+groupChildName+');\n'
        output += 'SmartAggregate '+piNodeName + ' = new SmartAggregate(' +SmartProjectName+', \"'+aggregateAttr + '\", \"'+groupAttr+'\", '+'Aggregator.Op.'+aggOp+');\n'
        output += 'return '+ piNodeName + ';'

print('transfer result:')
print(output)

with open(output_path,'w') as f:
    f.write(output)
