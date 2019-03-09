from collections import defaultdict
import sys
import pandas as pd
from itertools import combinations

def read_text_file():
    #reads the file into a pandas dataframe
    df = pd.read_csv(sys.argv[1],names=('Item', 'Basket'))
    #sort dataframe in ascending order of item number
    df.sort_values('Item',inplace = True)
    return df

def creating_baskets():
    # The baskets dictionary is required to get the true count of itemsets
    baskets = {}
    for index, row in df.iterrows():
        if row['Basket'] in baskets:
            baskets[row['Basket']].append(row['Item'])
        else:
            baskets[row['Basket']] = [row['Item']]
    return baskets

def frequent_1_items():
    c1 = [] #c1: candidate singletons
    l1 = [] #l1: truly frequent singletons

    #Construct: all_items->c1
    c1 = set(df['Item'])
    #Filter: c1->l1
    for candidate in c1:
        item_count = 0
        for key in baskets.keys():
            if candidate in baskets[key]:
                item_count += 1
        if item_count >= s:
            l1.append(candidate)
    return l1

def frequent_2_itemsets():
    c2 = [] #c2: candidate singletons
    l2 = [] #l2: truly frequent doubletons
    #Construct: l1->c2
    for i in range(len(l1)-1):
        for j in range(i+1,len(l1)):
            c2.append({l1[i],l1[j]})
    #Filter: c2->l2
    for candidate in c2:
        item_count = 0
        for key in baskets.keys():
            if candidate.issubset(set(baskets[key])):
                item_count += 1
        if item_count >= s:
            l2.append(candidate)
    return l2

def frequent_k_itemsets(prev,i):
    combination = []
    ck = []
    lk = []
    for w in range(len(prev)-1):
        for j in range(w+1,len(prev)):
            if prev[w] & prev[j]: #intersection
                if len(list(prev[w] & prev[j])) >= i-2:
                    combination.append(set(sorted(prev[w]|prev[j])))

    #Construct: lk->ck
    for element in combination:
        if element in ck:
            pass
        else:
            ck.append(element)
    #Filter: ck->lk
    for candidate in ck:
        item_count = 0
        for key in baskets.keys():
            if candidate.issubset(set(baskets[key])):
                item_count += 1
        if item_count >= s:
            lk.append(candidate)
    return lk

# Print frequent singletons
def print_frequent_1_items():
    if not l1:
        print('No subgraphs for these values of s and t!!')
    else:
        for f_itemset in l1:
            second_part = []
            final_second_part = []
            for key in baskets.keys():
                if f_itemset in baskets[key]:
                    second_part.append(key)
            second_part = (list(combinations(second_part,s)))
            for sets in second_part:
                print('{{{}}}{{{}}}'.format(f_itemset,','.join(sorted(sets))))

# Print frequent 2 itemsets or more
def print_frequent_k_itemsets(lk):
    if not lk:
	       print('No subgraphs for these values of s and t!!')
    else:
        for f_itemset in lk:
            second_part = []
            for key in baskets.keys():
                if f_itemset.issubset(set(baskets[key])):
                    second_part.append(key)
            for sets in list(combinations(second_part,s)):
                print('{{{}}}{{{}}}'.format(','.join(str(x) for x in list(f_itemset)),','.join(sorted(sets))))

if __name__ == "__main__":
    df = read_text_file()
    k = int(sys.argv[2]) # size of frequent itemset
    s = int(sys.argv[3]) #threshold

    #create baskets to make passes for count
    baskets = creating_baskets()
    #singletons
    if k == 1:
        #l1: truly frequent singletons
        l1 = frequent_1_items()
        print_frequent_1_items()
    #doubletons
    if k == 2:
        #l1: truly frequent singletons
        l1 = frequent_1_items()
        #l2: truly frequent doubletons
        l2 = frequent_2_itemsets()
        print_frequent_k_itemsets(l2)
    #frequent 3 itemsets or more
    if k > 2:
        #l1: truly frequent singletons
        l1 = frequent_1_items()
        #l2: truly frequent doubletons
        l2 = frequent_2_itemsets()
        prev = l2
        for i in range(3,k+1):
            #lk: truly frequent itemsets of size k
            lk =  frequent_k_itemsets(prev,i)
            prev = lk
        print_frequent_k_itemsets(lk)
