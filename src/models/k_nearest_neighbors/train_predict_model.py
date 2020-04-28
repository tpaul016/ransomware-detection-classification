import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.neighbors import KNeighborsClassifier
from sklearn import metrics
import pickle

df = pd.read_csv('../../features/processed_data_w_features.csv')
labels = np.array(df['Label'])

# TODO: dropping 'Tot Bytes' 'Tot Pkts' for now, have to go over their data
df.drop(['Label', 'Dst IP', 'Src IP', 'Start Time', 'End Time'], axis=1, inplace=True)

feature_list = list(df.columns)
features = np.array(df)
np.nan_to_num(features, copy=False)

# splitting data in training and testing sets
x_train, x_test, y_train, y_test = train_test_split(features, labels, test_size=0.3, random_state=42)
# train_features, test_features, train_labels, test_labels = train_test_split(features, labels, test_size=0.3, random_state=42)

# analyzing shape
print('Training Features Shape:', x_train.shape)
print('Training Labels Shape:', y_train.shape)
print('Testing Features Shape:', x_test.shape)
print('Testing Labels Shape:', y_test.shape)

# train the model
model = KNeighborsClassifier(n_neighbors=5)
model.fit(x_train, y_train)

# saving serialized format of model
pickle.dump(model, open('./k_nearest_neighbor_model.sav', 'wb'))

# predict the model
y_pred = model.predict(x_test)

# performance metrics
accuracy = metrics.accuracy_score(y_test, y_pred)
print("Accuracy: ",accuracy)
precision = metrics.precision_score(y_test, y_pred)
print("Precision: ",precision)
recall = metrics.recall_score(y_test, y_pred)
print("Recall: ", recall)
f1_score = metrics.f1_score(y_test, y_pred)
print("F1-Score: ", f1_score)
print(metrics.classification_report(y_test, y_pred))