###############################################################################
#
# RenderFarm : written by Anup Jayapal Rao 
#		e-mail id:	anup.kadam <at> gmail.com, anup_kadam <at> yahoo.com, ar210 <at> sussex.ac.uk
#
#	Written for the purpose of the Distributed systems assignment at the University Of Sussex, 2008.
#
###############################################################################

##
# FrameServer 
# 
# This program creates the FrameServer which collects rendered frames from all 
# the connected render clients. Upon collecting all the frames in a scene it
# combines the frames into a video clip using FFMPEG.
##

# System dependent libraries
import threading
import time
import socket
import os,os.path
import sys, logging
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
PATH_TO_FFMPEG= 'c:\\temp\\ffmpeg'

###############################################################################
# Class : GUI_FrameServer
# This class is responsible for creating and handling the GUI for the 
# FrameServer.
###############################################################################

gtk.gdk.threads_init()
	
class GUI_FrameServer( ):
	
	##
	# Constructor. Creates the widgets shown in the GUI for FrameServer.
	# @param	oRenderServer	RenderServer object that is controlling the renderfarm
	# @param	oFrameServer	FrameServer object that is represented by the GUI	
	def __init__( self, oRenderServer, oFrameServer):
		self.oRenderServer = oRenderServer
		self.oFrameServer = oFrameServer	
		self.win = gtk.Window()
		self.win.set_title('FrameServer on ' + socket.gethostname())
		self.win.connect('delete-event', gtk.main_quit)
		self.win.move(100, 100)
		#self.win.resize(800, 480)
		
		self.win.set_border_width(10)
		
		oVBox = gtk.VBox(False, 10)
		
		imageBanner = gtk.Image()
		imageBanner.set_from_file('FS.png')
		oVBox.pack_start(imageBanner,False,False)
		
		oHBox1 = gtk.HBox(False, 10)
		
		self.cmbVideoFormat = gtk.combo_box_new_text()
		self.cmbVideoFormat.append_text('mpg')
		self.cmbVideoFormat.append_text('mp4')
		self.cmbVideoFormat.append_text('flv')
		self.cmbVideoFormat.append_text('wmv')
		self.cmbVideoFormat.append_text('avi')
		self.cmbVideoFormat.set_active(0)
		oHBox1.pack_start(self.cmbVideoFormat,False,True)

		self.btnServerStart = gtk.Button('Start FrameServer')
		self.btnServerStart.connect("clicked", self.onStartServer, "Start FrameServer" )
		oHBox1.pack_end(self.btnServerStart,False,True)  	
  		
		oVBox.pack_start(oHBox1,False,False)
		
		oHBox2 = gtk.HBox(False, 10)
		
		label = gtk.Label("Frame Collection")
		label.set_justify(gtk.JUSTIFY_LEFT)
		oHBox2.pack_start(label,False,True)		
		
  		self.progressFrameCollection = gtk.ProgressBar()	
  		oHBox2.pack_end(self.progressFrameCollection,True,True)		
		
		oVBox.pack_start(oHBox2,False,False)	
		
		self.win.add(oVBox)
		
		self.win.show_all()	
		
	##
	# UpdateProgress. This function is used as a callback to update progress of the
	# Frameserver on the GUI. 
	# @param	nProgress		Percentage of completion
	# @param	nNextFrame		Next frame index being issued
	# @param	nEndFrameIndex	Last frame index of the animation scene	
	def UpdateProgress(self,nProgress, nNextFrame, nEndFrameIndex):
		strProgressText = str(nNextFrame) + " of "+ str(nEndFrameIndex) 
		
		gtk.gdk.threads_enter()
		self.progressFrameCollection.set_fraction(nProgress)
		self.progressFrameCollection.set_text(strProgressText)
		gtk.gdk.threads_leave()
					
	##
	# onStartServer. This method starts the FrameServer via the GUI. 
	# @param	widget		Widget triggering this function
	# @param	data		Trigger data
	def onStartServer(self,widget, data=None):
		bJobPending, JobFile, nFilesize, strExt, nStartFrameIndex, nEndFrameIndex = self.oRenderServer.GetJobDetails(0)
		
		if True == bJobPending:
			print "Frameserver running..."
			self.cmbVideoFormat.set_sensitive(False)
			self.btnServerStart.set_sensitive(False)
			self.oFrameServer.fxnProgressCallback = self.UpdateProgress
			self.oFrameServer.OutputFile = 'RenderFarm.'+self.cmbVideoFormat.get_active_text()
			self.oFrameServer.bTestMode = self.oRenderServer.IsInTestMode()
			self.oRenderServer.AddFrameServer(self.oFrameServer.GetName())
		else:
			# Following code snippet referred from official PyGTK-Demo examples
			dialog = gtk.MessageDialog(self.win,
				gtk.DIALOG_MODAL , gtk.MESSAGE_INFO, gtk.BUTTONS_OK,
				"Please start a job on the RenderServer.")
			dialog.run()
			dialog.destroy()
		
###############################################################################

class VideoRenderThread ( threading.Thread ):
	
	##
	# Constructor. Initialises a thread object for rendering a video.
	# @param	nJobIndex		Index of the Job	
	# @param	oJobMonitor		Job Monitor object monitoring the render farm	
	# @param	outfilename		The	name of the output video file
	# @param	strExt			The extension or format of the video file
	# @param	pathFrameFolder	The folder containing all the collected frames		
	def __init__( self, nJobIndex, oJobMonitor, outfilename, strExt, pathFrameFolder):
		global PATH_TO_FFMPEG
		threading.Thread.__init__(self)
		self.nJobIndex = nJobIndex
		self.oJobMonitor = oJobMonitor
		self.outfilename = outfilename
		self.strExt = strExt
		self.pathffmpeg = os.path.join(PATH_TO_FFMPEG,'ffmpeg')
		self.pathFrameFolder = os.path.join(os.getcwd(),pathFrameFolder)

	##
	# run. This method is used to run the thread.
	def run ( self ):	
		pathCWD = os.getcwd()
		os.chdir(self.pathFrameFolder)
		pid = call([self.pathffmpeg, '-i', r'%04d.'+self.strExt, self.outfilename])
		os.chdir(pathCWD)
		
		self.oJobMonitor.UpdateStatus(self.nJobIndex,'[FrameServer]',-1,RenderFarm.FRAME_SCENE_COMPLETE)     
		
		print "Video render complete"
		sys.exit(5)
		
###############################################################################
# Class : FrameServer
# This class implements the FrameServer which also behaves as a servant for
# the ORB. It collects the rendered frames from the all render clients and 
# creates a video render thread when it has received all the frames of the 
# animation.
###############################################################################

class FrameServer (RenderFarm__POA.iFrameServer):
	
	##
	# Constructor. Initialises the FrameServer object.
	# @param	strName			Name for the FrameServer	
	# @param	oRenderServer	Render Server object that is controlling the renderfarm		
	def __init__(self, strName, oRenderServer):
		self.fxnProgressCallback = None		
		self.strName  = strName
		
		self.oRenderServer = oRenderServer
		self.oJobMonitor  = None		
		
		self.FramesFolder = 'AllFrames'
		
		self.FileHandleList = {}
		self.JobList = {}
		
		self.OutputFile = 'RenderFarm.mpg'
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
	# CreateFrameHandle. This method creates a file handle for a rendered frame being transferred using
	# CORBA based calls.
	# @param	nJobIndex		Index of the Job
	# @param	nFrameIndex		Index of the rendered frame
	# @param	strhashDigest	Hash digest of the rendered frame file
	# @param	nFileSize		Size of the frame file		
	def CreateFrameHandle( self, nJobIndex, nFrameIndex, strhashDigest, nFileSize):
		print "For Job ", nJobIndex,": Creating frame",nFrameIndex,":",nFileSize,"bytes"
		
		if True == self.bTestMode:
			time.sleep(2)
					
		if not self.JobList.has_key(nJobIndex):
			bJobPending, JobFile, nFilesize, strExt, nStartFrameIndex, nEndFrameIndex = self.oRenderServer.GetJobDetails(nJobIndex)
			self.JobList[nJobIndex] = nEndFrameIndex,strExt
		
		if self.FileHandleList.has_key(nFrameIndex) :
			print "Frame already exists"
			return False
		
		strFileName = "%04d"%nFrameIndex+'.'+self.JobList[nJobIndex][1]
		if not os.path.exists(self.FramesFolder):
			os.mkdir(self.FramesFolder)		
			
		pathFileName = os.path.join(self.FramesFolder, strFileName) 
		
		self.FileHandleList[nFrameIndex] = [open(pathFileName,'wb'), pathFileName, strhashDigest, nFileSize, 0] 
				
		return True
		
	##
	# AppendFrameChunk. This method receives a chunk of the file containing the rendered
	# frame via CORBA based calls.
	# @param	nJobIndex		Index of the Job
	# @param	nFrameIndex		Index of the rendered frame
	# @param	nLength			Length of data of chunk of file
	# @param	seqChunk		Sequence containing chunk of file
	def AppendFrameChunk( self, nJobIndex, nFrameIndex, nLength, seqChunk):
		
		if True == self.bTestMode:
			time.sleep(2)
					
		bReturn = False
		if not self.FileHandleList.has_key(nFrameIndex) :
			print "Frame does not exists"
			return False		
		
		FileHandleList = self.FileHandleList[nFrameIndex][0]
		if None == FileHandleList :
			print "File handle does not exist"
			return False	
					
		nFileSize = self.FileHandleList[nFrameIndex][3]
		nNewLength = self.FileHandleList[nFrameIndex][4]+nLength
		
		FileHandleList.write(seqChunk)
		FileHandleList.flush()
				
		if nNewLength < nFileSize:
			bReturn = True
		else:
			FileHandleList.close()
			FileHandleList = None
			bReturn = False
		
		self.FileHandleList[nFrameIndex][4] = nNewLength					
		
		return bReturn
		
	##
	# IsFrameTransferOk. This method checks if the transfer of the rendered file
	# was successful.
	# @param	nJobIndex		Index of the Job
	# @param	nFrameIndex		Index of the rendered frame	
	def IsFrameTransferOk(self, nJobIndex, nFrameIndex):
		
		if True == self.bTestMode:
			time.sleep(2)
					
		if not self.FileHandleList.has_key(nFrameIndex) :
			return False		
		
		pathFileName = self.FileHandleList[nFrameIndex][1] 
		oFrameFile = open(pathFileName,'rb')
		oCompleteFrameBuffer = oFrameFile.read()
		oFrameFile.close()	
		
		# Generate MD5 hash 
		hashMD5 = hashlib.md5()		
		hashMD5.update(oCompleteFrameBuffer)
		hashDigest = hashMD5.hexdigest()			
		
		if hashDigest != self.FileHandleList[nFrameIndex][2]:
			del self.FileHandleList[nFrameIndex]
			return False
		
		if None != self.fxnProgressCallback:
			nProgress = float(nFrameIndex) / float(self.JobList[nJobIndex][0])
			self.fxnProgressCallback(nProgress, nFrameIndex, self.JobList[nJobIndex][0])
						
		self.oJobMonitor.UpdateStatus(nJobIndex,'[FrameServer]',nFrameIndex,RenderFarm.FRAME_COLLECTED)  
		self.oRenderServer.SetFrameAsComplete(nFrameIndex)
		
		if self.JobList[nJobIndex][0] == len(self.FileHandleList) :
			# 'RenderFarm.mpg'
			oVideoRenderThread = VideoRenderThread(nJobIndex,self.oJobMonitor,self.OutputFile,self.JobList[nJobIndex][1],self.FramesFolder )
			oVideoRenderThread.start()
						
		return True

###############################################################################
# Main application thread
# This thread initialises the ORB, binds the FrameServer object to the naming
# context and runs the GUI loop.
###############################################################################

# Obtains a local copy of FFMPEG if it does not already have one.
if not os.path.exists(PATH_TO_FFMPEG):
	print "Copying FFMPEG setup to a local destination..."
	shutil.copytree('H:\\a\\ar\\ar210\\WindowsProfile\\My Documents\\Portable\\ffmpeg',PATH_TO_FFMPEG)
	print "FFMPEG setup copied"
	
print "Frameserver starting..."
	
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
application_arguments.append("-ORBendPoint")
application_arguments.append("giop:tcp::8003")

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
	print "Did not locate RenderServer. Please start RenderServer before starting FrameServer."
	sys.exit(2)
	
# Bind a FrameServer object
servantFrameServer = FrameServer("FrameServer",oRenderServer)
oPOA.activate_object(servantFrameServer)
objrefFrameServer = servantFrameServer._this()		
oName = [CosNaming.NameComponent(servantFrameServer.GetName(),"obj")]
oNameRoot.rebind(oName,objrefFrameServer)

#Instantiate a GUI for the FrameServer
InstGUI = GUI_FrameServer(oRenderServer,servantFrameServer)

#GTK loop
gtk.gdk.threads_enter()
gtk.main()	
gtk.gdk.threads_leave()

# This part of GUI loop was referred from TK based tictactoe example provided with PyOmniOrb	
print "Shut down FrameServer"
oORB.shutdown(0)

# ffmpeg -i ..\..\SpringTerm\DS\Code\AllFrames\%04d.png outfile.ogg
# ffmpeg -i ..\..\SpringTerm\DS\Code\AllFrames\%04d.png outfile.flv
# ffmpeg -i ..\..\SpringTerm\DS\Code\AllFrames\%04d.png outfile.mpg
# ffmpeg -i ..\..\SpringTerm\DS\Code\AllFrames\%04d.png outfile.mp4
# ffmpeg -i ..\..\SpringTerm\DS\Code\AllFrames\%04d.png outfile.wmv
# ffmpeg -i ..\..\SpringTerm\DS\Code\AllFrames\%04d.png outfile.avi
# ffmpeg -i ..\..\SpringTerm\DS\Code\AllFrames\%04d.png -f ogg  -vcodec libtheora -vb 1024k outfile.ogg
