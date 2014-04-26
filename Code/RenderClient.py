###############################################################################
#
# RenderFarm : written by Anup Jayapal Rao 
#		e-mail id:	anup.kadam <at> gmail.com, anup_kadam <at> yahoo.com, ar210 <at> sussex.ac.uk
#
#	Written for the purpose of the Distributed systems assignment at the University Of Sussex, 2008.
#
###############################################################################

##
# RenderClient
# 
# This program creates the render clients of the renderfarm, which upon collection 
# of the 3D scene file, renders the scene using BLENDER. The completed frame is then
# forwarded to the FrameServer.
##

# System dependent libraries
import threading
import time
import socket
import os,os.path
import sys, logging
from optparse import OptionParser
import tftpy
from subprocess import *
import hashlib
import shutil 

# GUI related libraries
import pygtk
pygtk.require('2.0')
import gobject
import pango
import gtk
import math
import time
from gtk import gdk
import cairo   

# CORBA related libraries
import CORBA
import CosNaming

# IDL generated library
import RenderFarm
import RenderFarm__POA

# GLOBALS
tftpy.setLogLevel(logging.INFO)
PATH_TO_BLENDER = 'c:\\temp\\blender'

###############################################################################
# Class : GUI_RenderClient
# This class is responsible for creating and handling the GUI for the 
# RenderClient.
###############################################################################

gtk.gdk.threads_init()
	
class GUI_RenderClient( ):
	
	##
	# Constructor. Creates the widgets for the GUI of the RenderClient. 
	# @param	oRenderServer	RenderServer object that is controlling the renderfarm
	# @param	oFrameServer	FrameServer object that is collecting the rendered frames
	# @param	oJobMonitor		JobMonitor object that is monitoring the renderfarm
	# @param	oRenderClient	Render Client object that is represented by the GUI			
	def __init__( self, oRenderServer, oFrameServer, oJobMonitor, oRenderClient):
		self.oRenderServer = oRenderServer
		self.oFrameServer = oFrameServer		
		self.oJobMonitor = oJobMonitor			
		self.oRenderClient = oRenderClient	
		self.win = gtk.Window()
		self.win.set_title('RenderClient on ' + socket.gethostname())
		self.win.connect('delete-event', gtk.main_quit)
		self.win.move(100, 100)
		#self.win.resize(800, 480)
		
		self.win.set_border_width(10)
		
		oVBox = gtk.VBox(False, 10)
		
		imageBanner = gtk.Image()
		imageBanner.set_from_file('RC.png')
		oVBox.pack_start(imageBanner,False,False)
		
		oHBox1 = gtk.HBox(False, 10)
		
		self.lbl_RenderServer = gtk.Label("Render Server: Not connected")
		self.lbl_RenderServer.set_justify(gtk.JUSTIFY_LEFT)
		oHBox1.pack_start(self.lbl_RenderServer,False,True)	
		
		self.lbl_FrameServer = gtk.Label("Frames Server: Not connected")
		self.lbl_FrameServer.set_justify(gtk.JUSTIFY_LEFT)
		oHBox1.pack_start(self.lbl_FrameServer,True,True)		
  		
		self.btnServerStart = gtk.Button('Start RenderClient')
		self.btnServerStart.connect("clicked", self.onStartServer, "Start RenderClient" )
  		oHBox1.pack_end(self.btnServerStart,False,True)  	
  		
		oVBox.pack_start(oHBox1,False,False)
		
		oHBox2 = gtk.HBox(False, 10)
		
		label = gtk.Label("Download Progress")
		label.set_justify(gtk.JUSTIFY_LEFT)
		oHBox2.pack_start(label,False,True)		
		
  		self.progressDownload = gtk.ProgressBar()	
  		oHBox2.pack_start(self.progressDownload,True,True)		
		
		oVBox.pack_start(oHBox2,False,False)
		
		oHBox3 = gtk.HBox(False, 10)	
		
		self.Framelabel = gtk.Label("Current Frame")
		self.Framelabel .set_justify(gtk.JUSTIFY_LEFT)
		oHBox3.pack_start(self.Framelabel ,False,True)	
		
  		self.progressCurrentFrame = gtk.ProgressBar()	
  		oHBox3.pack_start(self.progressCurrentFrame,True,True)		
		
		oVBox.pack_start(oHBox3,False,False)		
		
		self.win.add(oVBox)
		
		self.win.show_all()	
			
	##
	# UpdateDownloadProgress 
	# @param	nCurrent	Number Of bytes transferred
	# @param	nTotal		Number Of total bytes to be transferred
	def UpdateDownloadProgress(self, nCurrent, nTotal):
		strProgressText = str(nCurrent) + " of "+ str(nTotal) + " bytes"
		
		gtk.gdk.threads_enter()
		self.progressDownload.set_fraction(float(nCurrent)/float(nTotal))
		self.progressDownload.set_text(strProgressText)
		gtk.gdk.threads_leave()
		
	##
	# UpdateFrameProgress. This function is used as a callback to update progress of the
	# RenderClient on the GUI. 
	# @param	nFrameIndex		Index of the frame completed
	# @param	nProgress		Percentage of completion
	# @param	strProgressText	Progress text	
	def UpdateFrameProgress(self, nFrameIndex, nProgress, strProgressText):
		if 0 < nFrameIndex:
			strProgressText = strProgressText+" ["+str(nFrameIndex) + "]"
		
		gtk.gdk.threads_enter()
		self.progressCurrentFrame.set_fraction(float(nProgress)/100)
		self.progressCurrentFrame.set_text(strProgressText)
		gtk.gdk.threads_leave()
				
	##
	# onStartServer. This method starts the RenderClient via the GUI. 
	# @param	widget		Widget triggering this function
	# @param	data		Trigger data
	def onStartServer(self,widget, data=None):
		self.btnServerStart.set_sensitive(False)
		self.lbl_RenderServer.set_label("Render Server: "+self.oRenderServer.GetHostName())
		self.lbl_FrameServer.set_label("Frame Server: "+self.oFrameServer.GetHostName())	
		self.oRenderClient.bTestMode = self.oRenderServer.IsInTestMode()		
		self.oRenderClient.oDownloadProgress.fxnProgressCallback = self.UpdateDownloadProgress	
		self.oRenderClient.fxnProgressCallback = self.UpdateFrameProgress	
		oRenderClientThread = RenderClientThread(self.oRenderServer,self.oFrameServer,self.oRenderClient )
		oRenderClientThread.start()

###############################################################################
# Class : Progress
# This class is responsible for handling callbacks in relation to the progress 
# of the TFTP download.
###############################################################################

class Progress(object):
	
	##
	# Constructor. Initialises the progress object. 
	# @param	out		Output stream
	def __init__(self, out):
		self.progress = 0
		self.out = out
		self.nFilesize = 1
		self.fxnProgressCallback = None
		
	##
	# progresshook. This method is calls the GUI to make updates on progress. 
	# @param	pkt		Packet containing progress data	
	def progresshook(self, pkt):
		self.progress += len(pkt.data)
		if None != self.fxnProgressCallback:
			self.fxnProgressCallback(self.progress,self.nFilesize)
		#self.out("Downloaded %d bytes" % self.progress)

###############################################################################
# Class : RenderClient
# This class implements the RenderClient which also behaves as a servant for
# the ORB. It renders the frame indicated by the RenderServer and forwards the 
# frame to the FrameServer.
###############################################################################

class RenderClient (RenderFarm__POA.iRenderClient):
	
	##
	# Constructor. Initialises the RenderClient object. 
	# @param	strName			Name for the RenderClient
	# @param	oJobMonitor		Job Monitor object monitoring the renderfarm
	def __init__(self,strName,oJobMonitor):
		global PATH_TO_BLENDER
		self.strName  = strName
		self.oJobMonitor = oJobMonitor
		
		self.pathBlenderExe = os.path.join(PATH_TO_BLENDER,'blender')
		self.SceneFolder = os.path.join(os.getcwd(),strName)
		self.outputfile = None
		self.strExt = None
		
		self.State = RenderFarm.TC_UNINITIALISED
		self.nJobIndex = 0
		
		self.oDownloadProgress = Progress(tftpy.logger.info)
		self.oDownloadProgress.fxnProgressCallback = None
		
		self.fxnProgressCallback = None
		self.bTestMode = False
			
	##
	# GetName. This method returns the user specified name for the RenderServer.  
	def GetName(self):
		return self.strName
		
	##
	# GetHostName. This method returns the hostname of the machine running the RenderServer. 
	def GetHostName(self):
		return socket.gethostname()
		
	##
	# AddJobMonitor. This method adds a Job Monitor to the RenderServer. 
	# @param	strJobMonitorName	Name of the Job Monitor
	def AddJobMonitor(self, strJobMonitorName):
		oJobMonitor = oORB.string_to_object("corbaname:rir:#"+strJobMonitorName+".obj")
		if None == oJobMonitor:
			print "Did not locate JobMonitor with name:",strJobMonitorName
			return False
	
		self.oJobMonitor = oJobMonitor
		return True	
			
	##
	# GetState. This method returns the state of a RenderClient. 
	def GetState(self):
		return self.State	
		
	##
	# SetState. This method sets the state of the RenderClient as the RenderServer's
	# control. It also updates the GUI to indicate the progress.
	# @param	nFrameIndex		Index of the Frame
	# @param	eState			State of the Render Client
	def SetState(self,nFrameIndex,eState):
		self.State = eState
		if RenderFarm.TC_WAITING == self.State :
				self.oJobMonitor.UpdateStatus(self.nJobIndex,self.GetHostName(),-1,RenderFarm.FRAME_WAITING)
						
		if None != self.fxnProgressCallback:
			if RenderFarm.TC_UNINITIALISED == self.State :
				self.fxnProgressCallback(nFrameIndex,0,'Uninitialised')
			if RenderFarm.TC_INITIALISED == self.State :
				self.fxnProgressCallback(nFrameIndex,5,'Initialised')
			if RenderFarm.TC_WAITING == self.State :
				self.fxnProgressCallback(nFrameIndex,10,'Waiting')
			if RenderFarm.TC_PREPARED == self.State :
				self.fxnProgressCallback(nFrameIndex,15,'Prepared')				
			if RenderFarm.TC_RENDER_IN_PROGRESS == self.State :
				self.fxnProgressCallback(nFrameIndex,20,'Render in Progress')		
			if RenderFarm.TC_FRAME_FORWARDING == self.State :
				self.fxnProgressCallback(nFrameIndex,90,'Forwarding Frame')											
			if RenderFarm.TC_COMPLETE == self.State :
				self.fxnProgressCallback(nFrameIndex,100,'Render Complete')																
		
	##
	# PerformJobtransaction. THis method initiates the TFTP based file download of the 3D scene.
	# @param	oRenderServer	Render Server object that is controlling the renderfarm
	def PerformJobtransaction(self,oRenderServer):
		JobFile = ' '
		bJobPending, JobFile, nFilesize, self.strExt, nStartFrameIndex, nEndFrameIndex = oRenderServer.GetJobDetails(self.nJobIndex)
				
		if True == self.bTestMode:
			time.sleep(2)	
			
		if True == bJobPending:
			# Start tftp client which must 
			# tftp code referenced from tftpy examples
			#progresshook = Progress(tftpy.logger.info).progresshook
			self.oDownloadProgress.nFilesize	= nFilesize
			
			tftp_options = {}
			tftp_options['blksize'] = 512
			if not os.path.exists(self.SceneFolder):
				os.mkdir(self.SceneFolder)			
			self.outputfile = os.path.join(self.SceneFolder, JobFile) 
			
			HostIPaddress = oRenderServer.GetHostIPaddress()
			FTPServerPort = oRenderServer.GetFTPServerPort()
			
			print HostIPaddress,FTPServerPort
			tclient = tftpy.TftpClient(HostIPaddress, FTPServerPort, tftp_options)		
			tclient.download(JobFile, self.outputfile, self.oDownloadProgress.progresshook) 
			
			self.oJobMonitor.UpdateStatus(self.nJobIndex,self.GetHostName(),-1,RenderFarm.FRAME_DOWNLOADING)       

			#self.State = RenderFarm.TC_PREPARED
			self.SetState(-1,RenderFarm.TC_PREPARED)
			
		else:
			self.oJobMonitor.UpdateStatus(self.nJobIndex,self.GetHostName(),-1,RenderFarm.FRAME_SCENE_COMPLETE)   
			self.SetState(-1,RenderFarm.TC_COMPLETE)
		
	##
	# Render. This method calls the local copy of the BLENDER executable to render specified frame of the specified scene.
	# @param	oFrameServer	FrameServer object that is collecting the rendered frames
	# @param	nFrameIndex		Index of the frame that must be rendered
	# @param	strSceneName	Name of the scene being rendered
	def Render( self, oFrameServer,nFrameIndex,strSceneName):
		print "Client will render Scene:",strSceneName,"Frame:",nFrameIndex
		strPrefix = 'render_'
		
		if True == self.bTestMode:
			time.sleep(10)	
		
		#time.sleep(100000)
		
		self.oJobMonitor.UpdateStatus(self.nJobIndex,self.GetHostName(),nFrameIndex,RenderFarm.FRAME_RENDERING) 
		self.SetState(nFrameIndex,RenderFarm.TC_RENDER_IN_PROGRESS)     
		pid = call([self.pathBlenderExe, '-b', self.outputfile, '-S', strSceneName, '-F', self.strExt, '-o', r'//'+strPrefix+'#', '-f', str(nFrameIndex),  '-x', '1'])
		
		#Transfer file to Frameserver
		#print oFrameServer
		
		strFileName = strPrefix+"%04d"%nFrameIndex+"%04d"%nFrameIndex+'.'+self.strExt
		strFileName = os.path.join(self.SceneFolder,strFileName)
		
		self.oJobMonitor.UpdateStatus(self.nJobIndex,self.GetHostName(),nFrameIndex,RenderFarm.FRAME_FORWARDING) 
		self.SetState(nFrameIndex,RenderFarm.TC_FRAME_FORWARDING)       
		
		oFrameFile = open(strFileName,'rb')
		oCompleteFrameBuffer = oFrameFile.read()
		oFrameFile.close()
		
		# Generate MD5 hash 
		hashMD5 = hashlib.md5()		
		hashMD5.update(oCompleteFrameBuffer)
		hashDigest = hashMD5.hexdigest()
		
		bTransfer= False
		nAttempt = 1
		while not bTransfer:
			print "Transfer attempt", nAttempt,"for", nFrameIndex
		
			oFrameBuffer = oCompleteFrameBuffer
			oFrameServer.CreateFrameHandle(self.nJobIndex, nFrameIndex, hashDigest, len(oFrameBuffer))
			bStatus = True
			nPacketSize = 10240
			nPacketCount = 0
			while bStatus:
				oSeq = None
				if len(oFrameBuffer) > nPacketSize:
					oSeq = oFrameBuffer[:nPacketSize]
					oFrameBuffer = oFrameBuffer[nPacketSize:]
				else:
					oSeq = oFrameBuffer
				bStatus = oFrameServer.AppendFrameChunk(self.nJobIndex,nFrameIndex,nPacketSize,oSeq)
				nPacketCount = nPacketCount + 1
				#print "Packet",nPacketCount,"transfer :",bStatus

			bTransfer  = oFrameServer.IsFrameTransferOk(self.nJobIndex, nFrameIndex)
			if True == bTransfer:
				os.remove(strFileName)
				break
				
			nAttempt = nAttempt + 1
			
		print "Client has finished transferring frame",nFrameIndex
	
		self.SetState(-1,RenderFarm.TC_PREPARED)
	
###############################################################################
# Render Client Thread
# This thread initialises is used to poll the next state from the RenderServer
# and render frames when requested.
###############################################################################

class RenderClientThread ( threading.Thread ):
	
	##
	# Constructor. Initialises a thread object to poll the RenderServer for state changes. 
	# @param	oRenderServer	Render Server object that is controlling the renderfarm		
	# @param	oFrameServer	FrameServer object that is collecting the rendered frames
	# @param	oRenderClient	Render Client object that is renddering the frame
	def __init__( self, oRenderServer, oFrameServer, oRenderClient):
		threading.Thread.__init__(self)
		self.oRenderServer = oRenderServer
		self.oFrameServer = oFrameServer
		self.oRenderClient = oRenderClient

	##
	# run. This method is used to run the thread. 
	def run ( self ):	
		while True:
				eState = self.oRenderClient.GetState() 
				newState =  self.oRenderServer.GetNextState( self.oRenderClient.GetName(), eState )
				self.oRenderClient.SetState( -1,newState ) 
				
				if RenderFarm.TC_WAITING == self.oRenderClient.GetState():
					print "Negotiating FTP with host..."
					self.oRenderClient.PerformJobtransaction(self.oRenderServer)
				
				if RenderFarm.TC_PREPARED == self.oRenderClient.GetState():
					strSceneName = ' '
					nFrameIndex,strSceneName =  self.oRenderServer.GetFrameToRender(self.oRenderClient.GetName(), strSceneName) 
					#print nFrameIndex,strSceneName 
					if RenderFarm.RENDERTASK_COMPLETE == nFrameIndex:
						self.oRenderClient.SetState( nFrameIndex, RenderFarm.TC_PREPARED ) 
					elif RenderFarm.RENDERJOB_COMPLETE == nFrameIndex:
						self.oRenderClient.SetState( nFrameIndex, RenderFarm.TC_COMPLETE )
					else:
						self.oRenderClient.Render(oFrameServer, nFrameIndex, strSceneName)
							
				if RenderFarm.TC_COMPLETE == self.oRenderClient.GetState():
					break
					
				time.sleep(1)
		
		print "Client has finished all render jobs."
		sys.exit(5)
	
###############################################################################
# Main application thread
# This thread initialises the ORB, binds the RenderClient object to the naming
# context and runs the GUI loop.
###############################################################################

# Obtains a local copy of BLENDER if it does not already have one.
if not os.path.exists(PATH_TO_BLENDER):
	print "Copying BLENDER setup to a local destination..."
	shutil.copytree('H:\\a\\ar\\ar210\\WindowsProfile\\My Documents\\Portable\\blender',PATH_TO_BLENDER)
	print "BLENDER setup copied"
			
# Initialisation parameters for OmniOrbPy
application_arguments = sys.argv[:]
#application_arguments.append("-ORBtraceLevel")
#application_arguments.append("25")
application_arguments.append("-ORBmaxGIOPVersion")
application_arguments.append("1.2")
application_arguments.append("-ORBgiopMaxMsgSize")
application_arguments.append("2097152")
application_arguments.append("-ORBstrictIIOP")
application_arguments.append("1")
application_arguments.append("-ORBInitRef")
application_arguments.append("NameService=corbaname::aldgate.tn.informatics.scitech.sussex.ac.uk:8000")
application_arguments.append("-ORBserverTransportRule") 
application_arguments.append("*   bidir,unix,tcp,ssl")
application_arguments.append("-ORBclientTransportRule") 
application_arguments.append("*   bidir,unix,tcp,ssl")
application_arguments.append("-ORBofferBiDirectionalGIOP")
application_arguments.append("1")
application_arguments.append("-ORBacceptBiDirectionalGIOP")
application_arguments.append("1")
# Must be disabled for client and enabled for server
#application_arguments.append("-ORBendPoint")
#application_arguments.append("giop:tcp::8002")

# Initialise the ORB and the POA
oORB = CORBA.ORB_init(application_arguments,CORBA.ORB_ID)
oPOA = oORB.resolve_initial_references("RootPOA")
oPOA._get_the_POAManager().activate()

# Find the name service
oNameRoot = oORB.resolve_initial_references("NameService")
oNameRoot = oNameRoot._narrow(CosNaming.NamingContext)
if None == oNameRoot:
	print "Did not locate a valid naming service. Please check setup."
	sys.exit(1)
	
# Bind a RenderServer object	
oRenderServer = oORB.string_to_object("corbaname:rir:#RenderServer.obj")
if None == oRenderServer:
	print "Did not locate RenderServer. Please start RenderServer before starting client."
	sys.exit(2)

# Bind a FrameServer object
strFrameServerName = oRenderServer.GetNameOfAssociatedFrameServer()
if '' == strFrameServerName:
	print "Did not locate FrameServer. Please start FrameServer before starting JobMonitor."
	sys.exit(3)	
	
strFrameServerName = oRenderServer.GetNameOfAssociatedFrameServer()
oFrameServer = oORB.string_to_object("corbaname:rir:#"+strFrameServerName+".obj")
if None == oFrameServer:
	print "Did not locate FrameServer. Please start FrameServer before starting client."
	sys.exit(3)
	
# Bind a JobMonitor object
oJobMonitor = oORB.string_to_object("corbaname:rir:#JobMonitor.obj")
if None == oJobMonitor:
	print "Did not locate JobMonitor. Please start JobMonitor before starting client."
	sys.exit(4)
		
servantRenderClient = RenderClient(socket.gethostname(),oJobMonitor)
idClient = oPOA.activate_object(servantRenderClient)
objrefRenderClient = servantRenderClient._this()	
oName = [CosNaming.NameComponent(servantRenderClient.GetName(),"obj")]
oNameRoot.rebind(oName,objrefRenderClient)

#Set intial status.
oJobMonitor.UpdateStatus(-1,servantRenderClient.GetName(),-1,RenderFarm.FRAME_CLIENT_IDLE)
			
#Instantiate a GUI for the RenderClient			
InstGUI = GUI_RenderClient(oRenderServer,oFrameServer,oJobMonitor,servantRenderClient)

#GTK loop
gtk.gdk.threads_enter()
gtk.main()	
gtk.gdk.threads_leave()

# This part of GUI loop was referred from TK based tictactoe example provided with PyOmniOrb	
print "Shut down RenderClient"
oORB.shutdown(0)

#Force exit from all threads
sys.exit(5)
