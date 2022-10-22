def zip_list(l1,l2):
    
    #[1,2,3]   [1,2,3]              [[1,1],[2,2],[3,3]]
    inner_list=[]

    for i in range(0,len(l1)):
        inner_list.append([l1[i],l2[i]])
    print(inner_list)
zip_list(["Arshia","Sreejit","Arsh"],["98","97","78"])

        
        
