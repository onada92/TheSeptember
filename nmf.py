import TS
import preProc as pp

from scipy.sparse import *
from scipy import *
from sklearn.decomposition import NMF
import numpy as np 
import pandas as pd
from sklearn.metrics import mean_squared_error as mse
from sklearn.metrics import mean_absolute_error as mae
from scipy.sparse.linalg import svds

rating_mat = pp.matrix_form()

edited_mat = rating_mat.as_matrix()
user_rating_mat = np.mean(edited_mat,axis=1)
mat_demeaned = edited_mat - user_rating_mat.reshape(-1,1)

U, sigma, Vt = svds(mat_demeaned, k = 50)
sigma = np.diag(sigma)
all_user_predicted_ratings = np.dot(np.dot(U, sigma), Vt) + user_rating_mat.reshape(-1, 1)
res1 = pd.DataFrame(all_user_predicted_ratings, columns = rating_mat.columns)



print("The new is: " ,rating_mat.head())


# model = NMF(n_components=20, max_iter=10, solver='mu',l1_ratio=0.1, alpha = 0.1, random_state = 1)
# W = model.fit_transform(mat)
# H = model.components_
# res = pd.DataFrame(dot(W,H), columns = mat.columns)


#Reviewing the results 
# print("The old is: " ,res1.head())
# print("The new is: ",size(res))
# mse_error = mse(res1, res)
# mae_error = mae(res1, res)
# print("The mse is: ", mse_error)
# print("The mae is: ", mae_error)

def recommend_movies(predictions_df, userID, movies_df, original_ratings_df, num_recommendations=5):
	# Get and sort the user's predictions
    user_row_number = userID # UserID starts at 1, not 0
    sorted_user_predictions = preds_df.iloc[user_row_number].sort_values(ascending=False) # UserID starts at 1
    
    # Get the user's data and merge in the movie information.
    user_data = original_ratings_df[original_ratings_df.person_id == (userID)]
    user_full = (user_data.merge(movies_df, how = 'left', left_on = 'npid', right_on = 'npid').
                     sort_values(['rating'], ascending=False))

    print ('User {0} has already rated {1} movies.'.format(userID, user_full.shape[0]))
    print ('Recommending highest {0} predicted ratings movies not already rated.'.format(num_recommendations))
    
    # Recommend the highest predicted rating movies that the user hasn't seen yet.
    recommendations = (movies_df[~movies_df['npid'].isin(user_full['npid'])].
         merge(pd.DataFrame(sorted_user_predictions).reset_index(), how = 'left',
               left_on = 'npid',
               right_on = 'npid').
         rename(columns = {user_row_number: 'Predictions'}).
         sort_values('Predictions', ascending = False).
                       iloc[:num_recommendations, :-1]
                      )

    return user_full, recommendations
