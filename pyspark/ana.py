#str1= "word”
#str2=“wood”


word1=input()
word2=input()

word1_list=[]
word2_list=[]
count=0


""" for j in word2:
        if(i==j):
            count=count+1

if(count==len(word1)):
        print("Anagram") """

for char in word1:
    word1_list.append(char)

for char1 in word2:
    word2_list.append(char1)

print(word1_list.sort())

    
    
    


    

    
