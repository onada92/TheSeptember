from surprise import SVD
from surprise import Dataset
from surprise.model_selection import cross_validate

data = Dataset.load_builtin('ml-100k')
print(type(data))

# Use the famous SVD algorithm.
#algo = SVD()

# Run 5-fold cross-validation and print results.
#cross_validate(algo, data, measures=['RMSE', 'MAE'], cv=5, verbose=True)