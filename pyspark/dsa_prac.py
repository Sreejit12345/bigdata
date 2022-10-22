#ip=abcabcdefabc     op=6
#ip=zabcd           #op=5
# Question is to find the longest length of consecutive substrings. Consider circular condition z and a is valid

str='abcuvwxyza'
ip=[]
for p in str:     #constructing a list from the elements of the string
    ip.append(p)

count=0

inner_list=[]
for i in range(0,len(ip)-1):                                                  #iterate till second last element to prevent out of index exception since we are comparing next element
    if(abs(ord(ip[i+1])-ord(ip[i]))==1 or (abs(ord(ip[i+1])-ord(ip[i])))==25):     # if difference in asci value is 1 or 25 (for circluar case) we increment count
        
        count=count+1
    else:
        inner_list.append(count+1)           # else signifies that a particular substring has ended so we add to inner_list. this list contains the length of each substring
        count=0                              # reset count for next substrings
        continue;    
    if(i==len(ip)-2):                          # this is to capture the last substring we identify the last substring if the element compared is the second last one
        inner_list.append(count+1)         # append length of last substring to inner list
        

print(sorted(inner_list,reverse=True)[0])   # sort inner_list in descending order and get first element





        


    
