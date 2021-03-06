'''
Created on May 22, 2015

@author: Corentin Berger
'''

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

import unittest
import socket
import errno

from DIRAC import S_OK
from DIRAC import DError

from DIRAC.Core.Security.ProxyInfo import getProxyInfo

from DIRAC.DataManagementSystem.Client.DataLoggingClient   import DataLoggingClient
from DIRAC.DataManagementSystem.Client.DataLoggingDecorator  import DataLoggingDecorator



"""
In this tests, we suppose that DatLoggingDB is created and empty
"""

def splitIntoSuccFailed( lfns ):
  """return some as successful, others as failed """
  localLfns = list( lfns )
  successful = dict.fromkeys( localLfns[0::2], {} )
  failed = dict.fromkeys( set( localLfns ) - set( successful ), {} )
  return successful, failed


class TestFileCatalog:
  @DataLoggingDecorator( argsPosition = ['self', 'files', 'targetSE'] )
  def addFile( self, lfns, seName, exceptionFlag ):
    """Adding new file, registering them into seName"""
    if not exceptionFlag:
      s, f = splitIntoSuccFailed( lfns )
    else :
      raise Exception( 'addFile exception' )
    return S_OK( {'Successful' : s, 'Failed' : f} )

  @DataLoggingDecorator( argsPosition = ['self', 'files', 'targetSE' ] )
  def addReplica( self, lfns, seName ):
    """Adding new replica, registering them into seName"""
    self.getFileSize( lfns )
    s, f = splitIntoSuccFailed( lfns )
    return S_OK( {'Successful' : s, 'Failed' : f} )

  @DataLoggingDecorator( argsPosition = ['self', 'files'], directInsert = True )
  def getFileSize( self, lfns ):
    """Getting file size"""

    s, f = splitIntoSuccFailed( lfns )
    return S_OK( {'Successful' : s, 'Failed' : f} )

class TestStorageElement:

  def __init__( self, seName ):
    self.seName = seName

  @DataLoggingDecorator( argsPosition = ['self', 'files', 'targetSE' ] )
  def putFile( self, lfns, src ):
    """Physicaly copying one file from src"""
    self.getFileSize( lfns )
    s, f = splitIntoSuccFailed( lfns )

    return S_OK( {'Successful' : s, 'Failed' : f} )

  @DataLoggingDecorator( argsPosition = ['self', 'files'], directInsert = True )
  def getFileSize( self, lfns ):
    """Getting file size"""

    s, f = splitIntoSuccFailed( lfns )

    return S_OK( {'Successful' : s, 'Failed' : f} )


class TestDataManager:
  @DataLoggingDecorator( argsPosition = ['self', 'files'], directInsert = True )
  def derror( self, lfns ):
    return DError( errno.ENOENT, 'the interesting technical message' )

  @DataLoggingDecorator( argsPosition = ['self', 'files', 'srcSE', 'targetSE', 'timeout'], directInsert = True )
  def replicateAndRegister( self, lfns, srcSE, dstSE, timeout, protocol = 'srm' ):
    """ replicate a file from one se to the other and register the new replicas"""
    fc = TestFileCatalog()
    se = TestStorageElement( dstSE )

    res = se.putFile( lfns, srcSE )

    successful = res['Value']['Successful']
    failed = res['Value']['Failed']

    for lfn in failed:
      failed.setdefault( lfn, {} )['Replicate'] = 'blablaMsg'

    res = fc.addReplica( successful, dstSE )

    failed.update( res['Value']['Failed'] )

    for lfn in res['Value']['Failed']:
      failed.setdefault( lfn, {} )['Register'] = 'blablaMsg'

    successful = {}
    for lfn in res['Value']['Successful']:
      successful[lfn] = { 'Replicate' : 1, 'Register' : 2}

    return S_OK( {'Successful' : successful, 'Failed' : failed} )


  @DataLoggingDecorator( argsPosition = ['self', 'files', 'localPath', 'targetSE' ], directInsert = True )
  def putAndRegister( self, lfns, localPath, dstSE, exceptionFlag = False ):
    """ Take a local file and copy it to the dest storageElement and register the new file"""
    fc = TestFileCatalog()
    se = TestStorageElement( dstSE )

    res = se.putFile( lfns, localPath )
    failed = res['Value']['Failed']
    successful = res['Value']['Successful']

    for lfn in failed:
      failed.setdefault( lfn, {} )['put'] = 'blablaMsg'

    res = fc.addFile( successful, dstSE, exceptionFlag )

    failed.update( res['Value']['Failed'] )

    for lfn in res['Value']['Failed']:
      failed.setdefault( lfn, {} )['Register'] = 'blablaMsg'

    successful = {}
    for lfn in res['Value']['Successful']:
      successful[lfn] = { 'put' : 1, 'Register' : 2}

    return S_OK( {'Successful' : successful, 'Failed' : failed} )


  @DataLoggingDecorator( argsPosition = ['self', 'tuple' ], getActionArgsFunction = 'Tuple' ,
                          tupleArgsPosition = ['files', 'physicalFile', 'fileSize', 'targetSE', 'fileGuid', 'checksum' ] )
  def registerFile( self, fileTuple, catalog = '' ):
    args = []
    for t in fileTuple :
      args.append( t[0] )
    s, f = splitIntoSuccFailed( args )
    # print 'suc %s fail %s' % ( s, f )
    return S_OK( {'Successful' : s, 'Failed' : f} )


class ClientA( object ):

  def __init__( self, lfns ):
    self.lfns = lfns

  def doSomething( self ):
    dm = TestDataManager()
    res = dm.replicateAndRegister( self.lfns, 'sourceSE', 'destSE', 1, protocol = 'aProtocol' )
    res = TestStorageElement( 'sourceSE' ).getFileSize( self.lfns )

class ClientB( object ):

  def doSomething( self ):
    dm = TestDataManager()
    res = dm.putAndRegister( ['/data/file1', '/data/file2', '/data/file3', '/data/file4'], '/local/path/', 'destSE' )
    s = res['Value']['Successful']
    res = TestFileCatalog().getFileSize( s )


class RaiseDecoratorException:

  @DataLoggingDecorator( argsPosition = ['self', 'files', 'timeout', 'srcSE'], getActionArgsFunction = 'toto', directInsert = True )
  def test( self ):
    return S_OK( 5 )


class ClientC( object ):

  def doSomething( self ):
    rde = RaiseDecoratorException()
    res = rde.test()
    return res

class ClientD( object ):

  def doSomething( self ):
    dm = TestDataManager()
    res = dm.putAndRegister( ['/data/file1', '/data/file2', '/data/file3', '/data/file4'], '/local/path/', 'destSE', exceptionFlag = True )
    s = res['Value']['Successful']
    res = TestFileCatalog().getFileSize( s )

class ClientE( object ):

  def doSomething( self ):
    dm = TestDataManager()
    dm.derror( ['/data/file1', '/data/file2', '/data/file3', '/data/file4'] )

class DataLoggingArgumentsTestCase( unittest.TestCase ):
  pass

class ClientACase ( DataLoggingArgumentsTestCase ):
  def setUp( self ):
    self.dlc = DataLoggingClient()

  def test_insertion_equal( self ):
    # we call some methods, they are going to be logged
    client = ClientA( ['/data/file1', '/data/file2', '/data/file3', '/data/file4'] )
    client.doSomething()

    # we get sequence from DataLoggingClient
    res = self.dlc.getSequenceByID( '1' )
    self.assertTrue( res['OK'], res.get( 'Message', 'OK' ) )
    sequenceOne = res['Value'][0]

    res = self.dlc.getSequenceByID( '2' )
    self.assertTrue( res['OK'], res.get( 'Message', 'OK' ) )
    sequenceTwo = res['Value'][0]

    # we compare results
    self.assertEqual( len( sequenceOne.methodCalls ), 5 )
    self.assertEqual( len( sequenceTwo.methodCalls ), 1 )

    hostName = socket.gethostname()
    self.assertEqual( sequenceOne.hostName.name, hostName )
    self.assertEqual( sequenceTwo.hostName.name, hostName )

    proxyInfo = getProxyInfo()
    if proxyInfo['OK']:
      proxyInfo = proxyInfo['Value']
      userName = proxyInfo.get( 'username' )
      group = proxyInfo.get( 'group' )

    if userName :
      self.assertEqual( sequenceTwo.userName.name, userName )

    if group :
      self.assertEqual( sequenceTwo.group.name, group )

    self.assertEqual( sequenceOne.caller.name, '__main__.ClientA.doSomething' )

    sequenceOne.methodCalls[0].actions.sort( key = lambda x: x.file.name )
    self.assertEqual( sequenceOne.methodCalls[0].name.name, 'TestDataManager.replicateAndRegister' )
    self.assertEqual( sequenceOne.methodCalls[0].actions[0].file.name, '/data/file1' )
    self.assertEqual( sequenceOne.methodCalls[0].actions[1].file.name, '/data/file2' )
    self.assertEqual( sequenceOne.methodCalls[0].actions[2].file.name, '/data/file3' )
    self.assertEqual( sequenceOne.methodCalls[0].actions[3].file.name, '/data/file4' )
    self.assertEqual( sequenceOne.methodCalls[0].actions[0].status, 'Failed' )
    self.assertEqual( sequenceOne.methodCalls[0].actions[1].status, 'Failed' )
    self.assertEqual( sequenceOne.methodCalls[0].actions[2].status, 'Successful' )
    self.assertEqual( sequenceOne.methodCalls[0].actions[3].status, 'Failed' )

    sequenceOne.methodCalls[1].actions.sort( key = lambda x: x.file.name )
    self.assertEqual( sequenceOne.methodCalls[1].name.name, 'TestFileCatalog.addReplica' )
    self.assertEqual( sequenceOne.methodCalls[1].actions[0].file.name, '/data/file1' )
    self.assertEqual( sequenceOne.methodCalls[1].actions[1].file.name, '/data/file3' )
    self.assertEqual( sequenceOne.methodCalls[1].actions[0].status, 'Failed' )
    self.assertEqual( sequenceOne.methodCalls[1].actions[1].status, 'Successful' )

    sequenceOne.methodCalls[2].actions.sort( key = lambda x: x.file.name )
    self.assertEqual( sequenceOne.methodCalls[2].name.name, 'TestFileCatalog.getFileSize' )
    self.assertEqual( sequenceOne.methodCalls[2].actions[0].file.name, '/data/file1' )
    self.assertEqual( sequenceOne.methodCalls[2].actions[1].file.name, '/data/file3' )
    self.assertEqual( sequenceOne.methodCalls[2].actions[0].status, 'Failed' )
    self.assertEqual( sequenceOne.methodCalls[2].actions[1].status, 'Successful' )

    sequenceOne.methodCalls[3].actions.sort( key = lambda x: x.file.name )
    self.assertEqual( sequenceOne.methodCalls[3].name.name, 'TestStorageElement.putFile' )
    self.assertEqual( sequenceOne.methodCalls[3].actions[0].file.name, '/data/file1' )
    self.assertEqual( sequenceOne.methodCalls[3].actions[1].file.name, '/data/file2' )
    self.assertEqual( sequenceOne.methodCalls[3].actions[2].file.name, '/data/file3' )
    self.assertEqual( sequenceOne.methodCalls[3].actions[3].file.name, '/data/file4' )
    self.assertEqual( sequenceOne.methodCalls[3].actions[0].status, 'Successful' )
    self.assertEqual( sequenceOne.methodCalls[3].actions[1].status, 'Failed' )
    self.assertEqual( sequenceOne.methodCalls[3].actions[2].status, 'Successful' )
    self.assertEqual( sequenceOne.methodCalls[3].actions[3].status, 'Failed' )

    sequenceOne.methodCalls[4].actions.sort( key = lambda x: x.file.name )
    self.assertEqual( sequenceOne.methodCalls[4].name.name, 'TestStorageElement.getFileSize' )
    self.assertEqual( sequenceOne.methodCalls[4].actions[0].file.name, '/data/file1' )
    self.assertEqual( sequenceOne.methodCalls[4].actions[1].file.name, '/data/file2' )
    self.assertEqual( sequenceOne.methodCalls[4].actions[2].file.name, '/data/file3' )
    self.assertEqual( sequenceOne.methodCalls[4].actions[3].file.name, '/data/file4' )
    self.assertEqual( sequenceOne.methodCalls[4].actions[0].status, 'Successful' )
    self.assertEqual( sequenceOne.methodCalls[4].actions[1].status, 'Failed' )
    self.assertEqual( sequenceOne.methodCalls[4].actions[2].status, 'Successful' )
    self.assertEqual( sequenceOne.methodCalls[4].actions[3].status, 'Failed' )

    sequenceTwo.methodCalls[0].actions.sort( key = lambda x: x.file.name )
    self.assertEqual( sequenceTwo.caller.name, '__main__.ClientA.doSomething' )
    self.assertEqual( sequenceTwo.methodCalls[0].name.name, 'TestStorageElement.getFileSize' )
    self.assertEqual( sequenceTwo.methodCalls[0].actions[0].file.name, '/data/file1' )
    self.assertEqual( sequenceTwo.methodCalls[0].actions[1].file.name, '/data/file2' )
    self.assertEqual( sequenceTwo.methodCalls[0].actions[2].file.name, '/data/file3' )
    self.assertEqual( sequenceTwo.methodCalls[0].actions[3].file.name, '/data/file4' )
    self.assertEqual( sequenceTwo.methodCalls[0].actions[0].status, 'Successful' )
    self.assertEqual( sequenceTwo.methodCalls[0].actions[1].status, 'Failed' )
    self.assertEqual( sequenceTwo.methodCalls[0].actions[2].status, 'Successful' )
    self.assertEqual( sequenceTwo.methodCalls[0].actions[3].status, 'Failed' )




class ClientBCase ( DataLoggingArgumentsTestCase ):
  def setUp( self ):
    self.dlc = DataLoggingClient()

  def test_insertion_equal( self ):
    # we call some methods, they are going to be logged
    client = ClientB()
    client.doSomething()

    res = self.dlc.getSequenceByID( '3' )
    self.assertTrue( res['OK'], res.get( 'Message', 'OK' ) )
    sequenceOne = res['Value'][0]

    res = self.dlc.getSequenceByID( '4' )
    self.assertTrue( res['OK'], res.get( 'Message', 'OK' ) )
    sequenceTwo = res['Value'][0]

    # we compare results
    self.assertEqual( len( sequenceOne.methodCalls ), 4 )
    self.assertEqual( len( sequenceTwo.methodCalls ), 1 )

    hostName = socket.gethostname()
    self.assertEqual( sequenceOne.hostName.name, hostName )
    self.assertEqual( sequenceTwo.hostName.name, hostName )

    proxyInfo = getProxyInfo()
    if proxyInfo['OK']:
      proxyInfo = proxyInfo['Value']
      userName = proxyInfo.get( 'username' )
      group = proxyInfo.get( 'group' )

    if userName :
      self.assertEqual( sequenceOne.userName.name, userName )
      self.assertEqual( sequenceTwo.userName.name, userName )

    if group :
      self.assertEqual( sequenceOne.group.name, group )
      self.assertEqual( sequenceTwo.group.name, group )

    self.assertEqual( sequenceOne.caller.name, '__main__.ClientB.doSomething' )

    sequenceOne.methodCalls[0].actions.sort( key = lambda x: x.file.name )
    self.assertEqual( sequenceOne.methodCalls[0].name.name, 'TestDataManager.putAndRegister' )
    self.assertEqual( sequenceOne.methodCalls[0].actions[0].file.name, '/data/file1' )
    self.assertEqual( sequenceOne.methodCalls[0].actions[1].file.name, '/data/file2' )
    self.assertEqual( sequenceOne.methodCalls[0].actions[2].file.name, '/data/file3' )
    self.assertEqual( sequenceOne.methodCalls[0].actions[3].file.name, '/data/file4' )
    self.assertEqual( sequenceOne.methodCalls[0].actions[0].status, 'Failed' )
    self.assertEqual( sequenceOne.methodCalls[0].actions[1].status, 'Failed' )
    self.assertEqual( sequenceOne.methodCalls[0].actions[2].status, 'Successful' )
    self.assertEqual( sequenceOne.methodCalls[0].actions[3].status, 'Failed' )

    sequenceOne.methodCalls[1].actions.sort( key = lambda x: x.file.name )
    self.assertEqual( sequenceOne.methodCalls[1].name.name, 'TestFileCatalog.addFile' )
    self.assertEqual( sequenceOne.methodCalls[1].actions[0].file.name, '/data/file1' )
    self.assertEqual( sequenceOne.methodCalls[1].actions[1].file.name, '/data/file3' )
    self.assertEqual( sequenceOne.methodCalls[1].actions[0].status, 'Failed' )
    self.assertEqual( sequenceOne.methodCalls[1].actions[1].status, 'Successful' )

    sequenceOne.methodCalls[2].actions.sort( key = lambda x: x.file.name )
    self.assertEqual( sequenceOne.methodCalls[2].name.name, 'TestStorageElement.putFile' )
    self.assertEqual( sequenceOne.methodCalls[2].actions[0].file.name, '/data/file1' )
    self.assertEqual( sequenceOne.methodCalls[2].actions[1].file.name, '/data/file2' )
    self.assertEqual( sequenceOne.methodCalls[2].actions[2].file.name, '/data/file3' )
    self.assertEqual( sequenceOne.methodCalls[2].actions[3].file.name, '/data/file4' )
    self.assertEqual( sequenceOne.methodCalls[2].actions[0].status, 'Successful' )
    self.assertEqual( sequenceOne.methodCalls[2].actions[1].status, 'Failed' )
    self.assertEqual( sequenceOne.methodCalls[2].actions[2].status, 'Successful' )
    self.assertEqual( sequenceOne.methodCalls[2].actions[3].status, 'Failed' )

    sequenceOne.methodCalls[3].actions.sort( key = lambda x: x.file.name )
    self.assertEqual( sequenceOne.methodCalls[3].name.name, 'TestStorageElement.getFileSize' )
    self.assertEqual( sequenceOne.methodCalls[3].actions[0].file.name, '/data/file1' )
    self.assertEqual( sequenceOne.methodCalls[3].actions[1].file.name, '/data/file2' )
    self.assertEqual( sequenceOne.methodCalls[3].actions[2].file.name, '/data/file3' )
    self.assertEqual( sequenceOne.methodCalls[3].actions[3].file.name, '/data/file4' )
    self.assertEqual( sequenceOne.methodCalls[3].actions[0].status, 'Successful' )
    self.assertEqual( sequenceOne.methodCalls[3].actions[1].status, 'Failed' )
    self.assertEqual( sequenceOne.methodCalls[3].actions[2].status, 'Successful' )
    self.assertEqual( sequenceOne.methodCalls[3].actions[3].status, 'Failed' )

    sequenceTwo.methodCalls[0].actions.sort( key = lambda x: x.file.name )
    self.assertEqual( sequenceTwo.caller.name, '__main__.ClientB.doSomething' )
    self.assertEqual( sequenceTwo.methodCalls[0].name.name, 'TestFileCatalog.getFileSize' )
    self.assertEqual( sequenceTwo.methodCalls[0].actions[0].file.name, '/data/file3' )
    self.assertEqual( sequenceTwo.methodCalls[0].actions[0].status, 'Successful' )


class ClientCCase ( DataLoggingArgumentsTestCase ):
  # this client raise an exception from the decorator, the result should be unchanged by the exception
  def setUp( self ):
    pass

  def test_no_exception( self ):
    client = ClientC()
    res = client.doSomething()
    self.assertEqual( res['Value'], 5 )

class ClientDCase ( DataLoggingArgumentsTestCase ):
  # this client raise an exception from a decorate method, the exception should be raise by the decorator
  def setUp( self ):
    self.dlc = DataLoggingClient()

  def test_no_exception( self ):
    client = ClientD()
    # we check if an exception is raised
    with self.assertRaises( Exception ):
      client.doSomething()

    res = self.dlc.getSequenceByID( '6' )
    self.assertTrue( res['OK'], res.get( 'Message', 'OK' ) )
    sequence = res['Value'][0]

    self.assertEqual( len( sequence.methodCalls ), 4 )

    hostName = socket.gethostname()
    self.assertEqual( sequence.hostName.name, hostName )

    proxyInfo = getProxyInfo()
    if proxyInfo['OK']:
      proxyInfo = proxyInfo['Value']
      userName = proxyInfo.get( 'username' )
      group = proxyInfo.get( 'group' )

    if userName :
      self.assertEqual( sequence.userName.name, userName )

    if group :
      self.assertEqual( sequence.group.name, group )

    self.assertEqual( sequence.caller.name, '__main__.ClientD.doSomething' )

    sequence.methodCalls[0].actions.sort( key = lambda x: x.file.name )
    self.assertEqual( sequence.methodCalls[0].name.name, 'TestDataManager.putAndRegister' )
    self.assertEqual( sequence.methodCalls[0].actions[0].file.name, '/data/file1' )
    self.assertEqual( sequence.methodCalls[0].actions[1].file.name, '/data/file2' )
    self.assertEqual( sequence.methodCalls[0].actions[2].file.name, '/data/file3' )
    self.assertEqual( sequence.methodCalls[0].actions[3].file.name, '/data/file4' )
    self.assertEqual( sequence.methodCalls[0].actions[0].status, 'Failed' )
    self.assertEqual( sequence.methodCalls[0].actions[1].status, 'Failed' )
    self.assertEqual( sequence.methodCalls[0].actions[2].status, 'Failed' )
    self.assertEqual( sequence.methodCalls[0].actions[3].status, 'Failed' )
    self.assertEqual( sequence.methodCalls[0].actions[0].errorMessage, 'addFile exception' )
    self.assertEqual( sequence.methodCalls[0].actions[1].errorMessage, 'addFile exception' )
    self.assertEqual( sequence.methodCalls[0].actions[2].errorMessage, 'addFile exception' )
    self.assertEqual( sequence.methodCalls[0].actions[3].errorMessage, 'addFile exception' )

    sequence.methodCalls[1].actions.sort( key = lambda x: x.file.name )
    self.assertEqual( sequence.methodCalls[1].name.name, 'TestFileCatalog.addFile' )
    self.assertEqual( sequence.methodCalls[1].actions[0].file.name, '/data/file1' )
    self.assertEqual( sequence.methodCalls[1].actions[1].file.name, '/data/file3' )
    self.assertEqual( sequence.methodCalls[1].actions[0].status, 'Failed' )
    self.assertEqual( sequence.methodCalls[1].actions[1].status, 'Failed' )
    self.assertEqual( sequence.methodCalls[1].actions[0].errorMessage, 'addFile exception' )
    self.assertEqual( sequence.methodCalls[1].actions[1].errorMessage, 'addFile exception' )

    sequence.methodCalls[2].actions.sort( key = lambda x: x.file.name )
    self.assertEqual( sequence.methodCalls[2].name.name, 'TestStorageElement.putFile' )
    self.assertEqual( sequence.methodCalls[2].actions[0].file.name, '/data/file1' )
    self.assertEqual( sequence.methodCalls[2].actions[1].file.name, '/data/file2' )
    self.assertEqual( sequence.methodCalls[2].actions[2].file.name, '/data/file3' )
    self.assertEqual( sequence.methodCalls[2].actions[3].file.name, '/data/file4' )
    self.assertEqual( sequence.methodCalls[2].actions[0].status, 'Successful' )
    self.assertEqual( sequence.methodCalls[2].actions[1].status, 'Failed' )
    self.assertEqual( sequence.methodCalls[2].actions[2].status, 'Successful' )
    self.assertEqual( sequence.methodCalls[2].actions[3].status, 'Failed' )

    sequence.methodCalls[3].actions.sort( key = lambda x: x.file.name )
    self.assertEqual( sequence.methodCalls[3].name.name, 'TestStorageElement.getFileSize' )
    self.assertEqual( sequence.methodCalls[3].actions[0].file.name, '/data/file1' )
    self.assertEqual( sequence.methodCalls[3].actions[1].file.name, '/data/file2' )
    self.assertEqual( sequence.methodCalls[3].actions[2].file.name, '/data/file3' )
    self.assertEqual( sequence.methodCalls[3].actions[3].file.name, '/data/file4' )
    self.assertEqual( sequence.methodCalls[3].actions[0].status, 'Successful' )
    self.assertEqual( sequence.methodCalls[3].actions[1].status, 'Failed' )
    self.assertEqual( sequence.methodCalls[3].actions[2].status, 'Successful' )
    self.assertEqual( sequence.methodCalls[3].actions[3].status, 'Failed' )


class ClientECase ( DataLoggingArgumentsTestCase ):
  # this client raise an exception from a decorate method, the exception should be raise by the decorator
  def setUp( self ):
    self.dlc = DataLoggingClient()

  def test_no_exception( self ):
    client = ClientE()
    client.doSomething()

    res = self.dlc.getSequenceByID( '7' )
    self.assertTrue( res['OK'], res.get( 'Message', 'OK' ) )
    sequence = res['Value'][0]

    self.assertEqual( len( sequence.methodCalls ), 1 )

    hostName = socket.gethostname()
    self.assertEqual( sequence.hostName.name, hostName )

    proxyInfo = getProxyInfo()
    if proxyInfo['OK']:
      proxyInfo = proxyInfo['Value']
      userName = proxyInfo.get( 'username' )
      group = proxyInfo.get( 'group' )

    if userName :
      self.assertEqual( sequence.userName.name, userName )

    if group :
      self.assertEqual( sequence.group.name, group )

    self.assertEqual( sequence.caller.name, '__main__.ClientE.doSomething' )

    sequence.methodCalls[0].actions.sort( key = lambda x: x.file.name )
    self.assertEqual( sequence.methodCalls[0].name.name, 'TestDataManager.derror' )
    self.assertEqual( sequence.methodCalls[0].actions[0].file.name, '/data/file1' )
    self.assertEqual( sequence.methodCalls[0].actions[1].file.name, '/data/file2' )
    self.assertEqual( sequence.methodCalls[0].actions[2].file.name, '/data/file3' )
    self.assertEqual( sequence.methodCalls[0].actions[3].file.name, '/data/file4' )
    self.assertEqual( sequence.methodCalls[0].actions[0].status, 'Failed' )
    self.assertEqual( sequence.methodCalls[0].actions[1].status, 'Failed' )
    self.assertEqual( sequence.methodCalls[0].actions[2].status, 'Failed' )
    self.assertEqual( sequence.methodCalls[0].actions[3].status, 'Failed' )
    self.assertEqual( sequence.methodCalls[0].actions[0].errorCode, 2 )
    self.assertEqual( sequence.methodCalls[0].actions[1].errorCode, 2 )
    self.assertEqual( sequence.methodCalls[0].actions[2].errorCode, 2 )
    self.assertEqual( sequence.methodCalls[0].actions[3].errorCode, 2 )
    self.assertEqual( sequence.methodCalls[0].actions[0].errorMessage, 'No such file or directory ( 2 : the interesting technical message)' )
    self.assertEqual( sequence.methodCalls[0].actions[1].errorMessage, 'No such file or directory ( 2 : the interesting technical message)' )
    self.assertEqual( sequence.methodCalls[0].actions[2].errorMessage, 'No such file or directory ( 2 : the interesting technical message)' )
    self.assertEqual( sequence.methodCalls[0].actions[3].errorMessage, 'No such file or directory ( 2 : the interesting technical message)' )



if __name__ == "__main__":

  suite = unittest.defaultTestLoader.loadTestsFromTestCase( ClientACase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( ClientBCase ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( ClientCCase ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( ClientDCase ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( ClientECase ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
