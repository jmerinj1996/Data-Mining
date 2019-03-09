Spark Version: 2.2.0
Python version: 2.7.12

## A)	APRIORI ALGORITHM: [60 points] (The code follows exactly this algorithm)
1.	Let k = 1, where k is the frequent itemset of size k
2.	Find frequent 1-itemsets
3.	Find frequent (k+1)-itemsets from k-itemsets 
a) For every two frequent k-itemsets with k-1 common items, generate a (k+1) itemset candidate if all its k-item subsets are frequent
b) Find support of candidates by scanning the baskets
c)Remove infrequent candidates
4.If found new frequent itemsets, k++, repeat step 3.

#### EXECUTION:
python bipartite.py <graph.txt> < k-itemset > < threshold >
 
## B)	SON/APRIORI ALGORITHM: [40 points] (The code follows exactly this algorithm)
#### PHASE 1
1.	Divide basket file into chunks
*	Each chunk is the fraction p of the entire file
2. Treat each chunk as a sample
3. Run Apriori algorithm on the sample
*	Local threshold = pt
*	t is the support threshold of entire file
4.	Collect frequent itemsets from each chunk
*	Take union of all the itemsets that have been found frequent for one or more chunks.
*	These form the candidate itemsets
#### PHASE 2
5.	Make a second pass through baskets
*	Collect counts of candidates
*	Select itemsets that have a support of atleast s
*	Return frequent itemsets

#### REQUIREMENT:
Pandas: pip install pandas
#### EXECUTION:
bin/spark-submit son.py <graph.txt> < k-itemset > < threshold >
