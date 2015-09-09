'''
Created on Sep 9, 2015

@author: coberger
'''

"""
  During a duration, create sequences and insert them thanks to the DLS client
"""

import random, time
from DIRAC.DataManagementSystem.Client.DataLoggingClient import DataLoggingClient
from DIRAC.DataManagementSystem.Client.DataLogging.DLSequence import DLSequence
from DIRAC.DataManagementSystem.Client.DataLogging.DLAction import DLAction
from DIRAC.DataManagementSystem.Client.DataLogging.DLFile import DLFile
from DIRAC.DataManagementSystem.Client.DataLogging.DLStorageElement import DLStorageElement
from DIRAC.DataManagementSystem.Client.DataLogging.DLMethodName import DLMethodName

maxDuration = 1800  # 30mn

hostname = 'volhcb38.cern.ch'
portList = [ 9165, 9166, 9168, 9169]
ind = random.randint( 0, len( portList ) - 1 )
port = portList[ind]
servAddress = 'dips://%s:%s/DataManagement/DataLogging' % ( hostname, port ) + 'Bis' * ind


randomMax = 100000
randomMethocall = 20

file = '/lhcb/data/file'
targetSE = 'targetSE'
blob = 'physicalFile = blablablablablabla ,fileSize = 6536589'
srcSE = 'srcSE'

def makeSequence():
  sequence = DLSequence()
  sequence.setCaller( 'longCallerName' + str( random.randint( 0, 20 ) ) )
  calls = []
  calls.append( sequence.appendMethodCall( {'name': DLMethodName( 'longMethodName' + str( random.randint( 0, randomMethocall ) ) )} ) )
  calls.append( sequence.appendMethodCall( {'name': DLMethodName( 'longMethodName' + str( random.randint( 0, randomMethocall ) ) )} ) )
  calls.append( sequence.appendMethodCall( {'name': DLMethodName( 'longMethodName' + str( random.randint( 0, randomMethocall ) ) )} ) )
  sequence.popMethodCall()
  calls.append( sequence.appendMethodCall( {'name': DLMethodName( 'longMethodName' + str( random.randint( 0, randomMethocall ) ) )} ) )
  sequence.popMethodCall()
  sequence.popMethodCall()
  calls.append( sequence.appendMethodCall( {'name': DLMethodName( 'longMethodName' + str( random.randint( 0, randomMethocall ) ) )} ) )
  sequence.popMethodCall()
  calls.append( sequence.appendMethodCall( {'name': DLMethodName( 'longMethodName' + str( random.randint( 0, randomMethocall ) ) )} ) )
  sequence.popMethodCall()
  sequence.popMethodCall()

  files = []
  for x in range( 4 ):
    files.append( file + str( random.randint( 0, randomMax ) ) + '.data' )

  sources = []
  for x in range( 4 ):
    sources.append( srcSE + str( random.randint( 0, randomMax ) ) )

  targets = []
  for x in range( 4 ):
    targets.append( targetSE + str( random.randint( 0, randomMax ) ) )

  for call in calls :
    for x in range( 2 ):
      call.addAction( DLAction( DLFile( files[x * 2] ) , 'Successful' ,
              DLStorageElement( sources[x * 2] ),
               DLStorageElement( targets[x * 2] ),
              blob, None, None ) )
      call.addAction( DLAction( DLFile( files[x * 2 + 1 ] ) , 'Failed',
              DLStorageElement( sources[x * 2 + 1 ] ),
               DLStorageElement( targets[x * 2 + 1] ),
              blob, 'errorMessage', random.randint( 1, 1999 ) ) )
  return sequence


done = False
start = time.time()
client = DataLoggingClient( url = servAddress )

while not done :
  seq = makeSequence()
  res = client.insertSequence( seq )
  if not res['OK']:
    print 'error %s' % res['Message']
  if ( time.time() - start > maxDuration ):
    done = True
