'''
Created on Sep 9, 2015

@author: coberger
'''

"""
  Create SQL statements and print them for the Data Loggign System Database
"""

import random
import datetime

nb_se = 1000
nb_file = 1000000
nb_caller = 20
nb_group = 20
nb_host = 20
nb_user = 20
nb_method = 30
nb_sequences = 15000000
# number of method calls per sequence
nb_method_calls = 5
min_nb_actions = 1
max_nb_actions = 10
print "SET FOREIGN_KEY_CHECKS = 0;"
print "SET UNIQUE_CHECKS = 0;"
print "SET AUTOCOMMIT = 0;"

status = ['Successful', 'Failed']

def generate_sub_tables():
  print "START TRANSACTION;"
  for i in range( nb_caller ):
    print "INSERT INTO DLCaller (name) VALUES ('%s');" % ( 'caller' + str( i ) )

  for i in range( nb_group ):
    print "INSERT INTO DLGroup (name) VALUES ('%s');" % ( 'group' + str( i ) )

  for i in range( nb_user ):
    print "INSERT INTO DLUserName (name) VALUES ('%s');" % ( 'userName' + str( i ) )

  for i in range( nb_host ):
    print "INSERT INTO DLHostName (name) VALUES ('%s');" % ( 'hostName' + str( i ) )

  for i in range( nb_method ):
    print "INSERT INTO DLMethodName (name) VALUES ('%s');" % ( 'class.methodName' + str( i ) )

  for i in range( nb_se ):
    print "INSERT INTO DLStorageElement (name) VALUES ('%s');" % ( 'se' + str( i ) )

  for i in range( nb_file ):
    print "INSERT INTO DLFile (name) VALUES ('%s');" % ( 'File' + str( i ) )
  print "COMMIT;"


def generate_sequences():
  toPrint = True
  print "START TRANSACTION;"
  seq = "INSERT INTO DLSequence (callerID, groupID, userNameID, hostNameID) VALUES "
  mc = 'INSERT INTO DLMethodCall (creationTime, methodNameID, parentID, sequenceID, rank) VALUES'
  actions = "INSERT INTO `DLAction` (`methodCallID`, `fileID`, status, `srcSEID`, `targetSEID`, extra, `errorMessage`, `errorCode`) VALUES"
  for i in range( nb_sequences ) :
    toPrint = True
    seq += '(%s, %s, %s, %s),' % ( random.randint( 1, nb_caller ), random.randint( 1, nb_group ), random.randint( 1, nb_user ), random.randint( 1, nb_host ) )
    for j in range( nb_method_calls ):
      mc += " ('%s', %s, %s, %s, %s)," % ( datetime.datetime.utcnow().replace( microsecond = 0 ), random.randint( 1, 30 ), '%s' % ( 'null' if j == 0 else ( i * nb_method_calls + 1 ) ), i + 1 , 0 )
      for a in range( random.randint( min_nb_actions, max_nb_actions ) ) :
          actions += "(%s, %s, '%s', %s, %s, '%s', '%s', %s),"\
            % ( ( i ) * nb_method_calls + j + 1 , random.randint( 1, nb_file ), status[random.randint( 0, 1 )], random.randint( 1, nb_se ), random.randint( 1, nb_se ),
              'extra', 'errorMessage', random.randint( 1, 999 ) )
    if ( i % 1000 ) == 0 :
      seq = seq[:-1]
      seq += ';'
      mc = mc[:-1]
      mc += ';'
      actions = actions[:-1]
      actions += ';'
      print seq
      print mc
      print actions
      print "COMMIT;"
      print "START TRANSACTION;"
      seq = "INSERT INTO DLSequence (callerID, groupID, userNameID, hostNameID) VALUES "
      mc = 'INSERT INTO DLMethodCall (creationTime, methodNameID, parentID, sequenceID, rank) VALUES'
      actions = "INSERT INTO `DLAction` (`methodCallID`, `fileID`, status, `srcSEID`, `targetSEID`, extra, `errorMessage`, `errorCode`) VALUES"
      toPrint = False
  if toPrint :
    seq = seq[:-1]
    seq += ';'
    mc = mc[:-1]
    mc += ';'
    actions = actions[:-1]
    actions += ';'
    print seq
    print mc
    print actions
  print "COMMIT;"


generate_sequences()


print "SET FOREIGN_KEY_CHECKS = 1;"
print "SET UNIQUE_CHECKS = 1;"
print "SET AUTOCOMMIT = 1;"
