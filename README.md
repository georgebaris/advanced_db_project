Αυτό είναι το αποθετήριο για την εξαμηνιαία εργασία του μαθήματος "Προχωρημένα Θέματα Βάσεων Δεδομένων". 

Γεώργιος Μπαρής

Ιωάννης Κωνσταντίνος Χατζής

ΣΧΟΛΗ ΗΛΕΚΤΡΟΛΟΓΩΝ ΜΗΧΑΝΙΚΩΝ ΚΑΙ ΜΗΧΑΝΙΚΩΝ ΥΠΟΛΟΓΙΣΤΩΝ
ΕΘΝΙΚΟ ΜΕΤΣΟΒΙΟ ΠΟΛΥΤΕΧΝΙΟ
ΕΤΟΣ: 2024

Όλοι οι κώδικες βρίσκονται στον φάκελο "source".
Αφού έχει ολοκληρωθεί η εγκατάσταση του Spark σε Virtual Machines ή local ακολουθώντας ενδεικτικά τα βήματα του οδηγού: https://colab.research.google.com/drive/1pjf3Q6T-Ak2gXzbgoPpvMdfOHd1GqHZG?usp=sharing#scrollTo=I0jwIL1Ba-DU ,
πρέπει να δημιουργηθεί ένας φάκελος στον οποίο θα τοποθετηθούν όλα τα .csv αρχεία . 
Τα αρχεία που απαιτούνται για την εκτέλεση της άσκησης μπορούν να βρεθούν στους παρακάτω συνδέσμους:
  1. https://catalog.data.gov/dataset/crime-data-from-2010-to-2019
  2. https://catalog.data.gov/dataset/crime-data-from-2020-to-present
  3. http://www.laalmanac.com/employment/em12c_2015.php
  4. http://www.dblab.ece.ntua.gr/files/classes/data.tar.gz

Έπειτα πρέπει αυτά τα αρχεία να προστεθούν στο dfs με την εντολή:  hdfs dfs -put "FILENAME".csv hdfs:///user/user/.
Πριν την εκτέλεση των queries είναι απαραίτητο να εκτελεστούν οι παρακάτω εντολές ώστε να δημιουργηθούν τα απαραίτητα .parquet αρχεία:
  1. spark-submit create_dataframe.py
  2. spark-submit lapd_dataframe.py
  3. spark-submit geo_dataframe.py
  4. spark-submit income_dataframe.py

Τώρα μπορεί να εκτελεστεί οποιοδήποτε query με την εντολή: spark-submit "FILENAME"
   
  
