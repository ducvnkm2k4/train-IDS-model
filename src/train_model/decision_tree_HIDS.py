import pandas as pd
from sklearn.tree import DecisionTreeClassifier
from sklearn.metrics import confusion_matrix, accuracy_score, classification_report

def decision_tree_hids():
    # Load the dataset
    print('loading data train')
    data_train = pd.read_csv('src/output/data_raw/HIDS_train.csv')
    print('loading data test')
    data_test = pd.read_csv('src/output/data_raw/HIDS_test.csv')
    
    # Split the data into features and labels
    X_train = data_train.drop(columns=['label'])
    y_train = data_train['label']
    X_test = data_test.drop(columns=['label'])
    y_test = data_test['label']
    
    # Initialize the Decision Tree Classifier
    clf = DecisionTreeClassifier(random_state=42)
    
    # Fit the model
    print('fitting the model')
    clf.fit(X_train, y_train)
    
    # Make predictions on the test set
    print('making predictions')
    y_pred = clf.predict(X_test)
    
    # Evaluate the model
    print("Confusion Matrix:",confusion_matrix(y_test, y_pred))
    print("Accuracy Score:",accuracy_score(y_test, y_pred))
    print("Classification Report:",classification_report(y_test, y_pred))

if __name__ == "__main__":
    decision_tree_hids()