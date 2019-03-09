from pyspark import SparkContext
import sys
from itertools import combinations

def printf(x):
	print(list(x))

# Print frequent singletons
def print_frequent_1_items():
    if not global_l1:
        print('No subgraphs for these values of s and t!!')
    for f_itemset in global_l1:
        second_part = []
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
    for f_itemset in lk:
        second_part = []
        for key in baskets.keys():
            if f_itemset.issubset(set(baskets[key])):
                second_part.append(key)
        for sets in list(combinations(second_part,s)):
                        print('{{{}}}{{{}}}'.format(','.join(str(x) for x in list(f_itemset)),','.join(sorted(sets))))

def frequent_1_items(partition):
        basket_names = [] #names of baskets in each partition
        local_c1 = []
        local_l1 = []
        num_of_baskets_chunk = 0
        partit = list(partition)
		#Construct: all_items->c1
        for parts in partit:
                basket_names.append(parts[0])
                for item in parts[1]:
                        if item not in local_c1:
                                local_c1.append(item)
        local_c1.sort()
        local_thres = int(s*len(partit)/float(basket_num))
		#Filter: c1->l1
        for candidate in local_c1:
                item_count = 0
                for key in baskets.keys():
                        if key in basket_names:
                                if candidate in baskets[key]:
                                        item_count += 1
                if item_count >= local_thres:
                        local_l1.append(candidate)
        if k > 1:
                return basket_names,local_l1 ,local_thres
        return local_l1

def frequent_2_itemsets(partition):
        basket_names,local_l1,local_thres = frequent_1_items(partition)
        local_c2 = []
        local_l2 = []
		#Construct: l1->c2
        for i in range(len(local_l1)-1):
                for j in range(i+1,len(local_l1)):
                        local_c2.append({local_l1[i],local_l1[j]})
		#Filter: c2->l2
        for candidate in local_c2:
                item_count = 0
                for key in baskets.keys():
                        if key in basket_names:
                                if candidate.issubset(set(baskets[key])):
                                        item_count += 1
                if item_count >= local_thres:
                        local_l2.append(candidate)
        if k > 2:
                return basket_names,local_l2,local_thres
        return local_l2

def frequent_k_itemsets(partition):
        basket_names,prev,local_thres = frequent_2_itemsets(partition)
        for i in range(3,k+1):
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
                                if key in basket_names:
                                        if candidate.issubset(set(baskets[key])):
                                                item_count += 1
                        if item_count >= local_thres:
                                lk.append(candidate)
                prev = lk
        return lk

def creating_baskets():
		# The baskets dictionary is required to get the true count of itemsets
        baskets =lines.map(lambda x:x.split(',')).filter(lambda l:len(l)==2).map(lambda kv:(str(kv[1]),int(kv[0]))).groupByKey().mapValues(list)
        return baskets
def frequent_1_phase2(global_c1):
        l1 = []
        for candidate in global_c1:
                global_count = 0
                for key in baskets.keys():
                         if candidate in baskets[key]:
                                global_count += 1
                if global_count >= s:
                        l1.append(candidate)
        return l1

#get global count: Make a second pass through baskets
def frequent_k_phase2(global_comb):
        global_ck = []
        global_lk = []
        for element in global_comb:
                if element in global_ck:
                        pass
                else:
                        global_ck.append(element)
        for candidate in global_ck:
                global_count = 0
                for key in baskets.keys():
                        if candidate.issubset(set(baskets[key])):
                                global_count += 1
                if global_count >= s:
                        global_lk.append(candidate)
        return global_lk

if __name__ == "__main__":

        sc = SparkContext(appName ='inf553')
        NO_OF_CHUNKS = 3
        lines = sc.textFile(sys.argv[1],NO_OF_CHUNKS)
        basket_chunks = lines.map(lambda x:x.split(',')).filter(lambda l:len(l)==2).map(lambda kv:(str(kv[1]),int(kv[0]))).groupByKey().mapValues(list)

        k = int(sys.argv[2]) # size of frequent itemset
        s = int(sys.argv[3]) #global threshold

		#count the number of baskets
        basket_num = basket_chunks.count()
		#create baskets to make passes for count
        baskets = dict(creating_baskets().collect())

		#singletons
        if k==1:
                global_c1 = basket_chunks.mapPartitions(frequent_1_items).collect()
                global_l1 = frequent_1_phase2(set(global_c1))
                print_frequent_1_items()
		#doubletons
        if k == 2:
                global_c2 = basket_chunks.mapPartitions(frequent_2_itemsets).collect()
                global_l2 = frequent_k_phase2(global_c2)
                print_frequent_k_itemsets(global_l2)
		#frequent 3 itemsets or more
        if k > 2:
                global_ck = basket_chunks.mapPartitions(frequent_k_itemsets).collect()
                global_lk = frequent_k_phase2(global_ck)
                print_frequent_k_itemsets(global_lk)
