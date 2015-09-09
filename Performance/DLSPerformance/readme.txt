There is two script to test performances of the DLS

generateSQL : generate the sql statements to insert rows, the databs should be empty, but you can modify it and add an offset if there is already some rows in the database

generateSequences : generate DLSequence object and insert them.
It is possible to plots performances, for that use the script makeplot.py from DFCPerfomance package
Files shall have this structure :
Date_of_begin		Date_of_end		Time_between_begin_and_end

Look at options provide by this script, you can specify the destination file, the data file, etc...