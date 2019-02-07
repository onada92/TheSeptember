import TS
import preProc as pp

import pandas as pd
import tensorflow.keras as keras
import tensorflow as tf 
import matplotlib.pyplot as plt
from surprise import SVD, NMF
from surprise import evaluate, Dataset, accuracy
from surprise import Reader
from surprise.model_selection import cross_validate, train_test_split, KFold
from collections import defaultdict

def get_top_n(predictions, n=5):
    '''Return the top-N recommendation for each user from a set of predictions.

    Args: 
        predictions(list of Prediction objects): The list of predictions, as
            returned by the test method of an algorithm.
        n(int): The number of recommendation to output for each user. Default
            is 10.

    Returns:
    A dict where keys are user (raw) ids and values are lists of tuples:
        [(raw item id, rating estimation), ...] of size n.
    '''

    # First map the predictions to each user.
    top_n = defaultdict(list)
    for uid, iid, true_r, est, _ in predictions:
        top_n[uid].append((iid, est))

    # Then sort the predictions for each user and retrieve the k highest ones.
    for uid, user_ratings in top_n.items():
        user_ratings.sort(key=lambda x: x[1], reverse=True)
        top_n[uid] = user_ratings[:n]

    return top_n

def precision_recall_at_k(predictions, k=10, threshold=3.5):
    '''Return precision and recall at k metrics for each user.'''

    # First map the predictions to each user.
    user_est_true = defaultdict(list)
    for uid, _, true_r, est, _ in predictions:
        user_est_true[uid].append((est, true_r))

    precisions = dict()
    recalls = dict()
    for uid, user_ratings in user_est_true.items():

        # Sort user ratings by estimated value
        user_ratings.sort(key=lambda x: x[0], reverse=True)

        # Number of relevant items
        n_rel = sum((true_r >= threshold) for (_, true_r) in user_ratings)

        # Number of recommended items in top k
        n_rec_k = sum((est >= threshold) for (est, _) in user_ratings[:k])

        # Number of relevant and recommended items in top k
        n_rel_and_rec_k = sum(((true_r >= threshold) and (est >= threshold))
                              for (est, true_r) in user_ratings[:k])

        # Precision@K: Proportion of recommended items that are relevant
        precisions[uid] = n_rel_and_rec_k / n_rec_k if n_rec_k != 0 else 1

        # Recall@K: Proportion of relevant items that are recommended
        recalls[uid] = n_rel_and_rec_k / n_rel if n_rel != 0 else 1

    return precisions, recalls
rating_mat = pp.merges()
rating_mat = rating_mat[['person_id','npid','rating']]
rating_mat = pd.DataFrame(rating_mat)
rating_mat = rating_mat[:600000]
#rating_mat = rating_mat[rating_mat.uid == '50161001' and rating != 5]
#print(rating_mat)
#print(rating_mat.shape)
reader = Reader()
data = Dataset.load_from_df(rating_mat[['person_id','npid','rating']], reader)
kf = KFold(n_splits = 5)
trainset = data.build_full_trainset()

# #data.split(n_folds=5)
# #trainset, testset = train_test_split(data, test_size = 0.2)
algo = SVD(n_factors = 40)
algo.fit(trainset)
testset = trainset.build_anti_testset()
predictions = algo.test(testset)
top_n = get_top_n(predictions, n=10)
# #print(predictions)
# for uid, user_ratings in top_n.items():
#     print(uid, [iid for (iid, _) in user_ratings])
# evaluate(algo, data, measures=['RMSE'])
# accuracy.rmse(predictions)
cross_validate(algo, data, measures=['RMSE', 'MAE'], cv=5, verbose=True)
recs = []
precs = []
srecs = 0
sprecs = 0
for trainset, testset in kf.split(data):
    algo.fit(trainset)
    predictions = algo.test(testset)
    precisions, recalls = precision_recall_at_k(predictions, k=5, threshold=4)

    # Precision and recall can then be averaged over all users
    #print(sum(prec for prec in precisions.values()) / len(precisions))
    x = sum(prec for prec in precisions.values()) / len(precisions)
    precs.append(x)
    #print(sum(rec for rec in recalls.values()) / len(recalls))
    y = sum(rec for rec in recalls.values()) / len(recalls)
    recs.append(y) 

for i in range(len(precs)):
	srecs = srecs +recs[i]
	sprecs = sprecs + precs[i]
	arecs = srecs/len(recs)
	aprecs = sprecs/len(precs)

print("The average recall is: ",arecs)
print("The average precision is: ", aprecs)