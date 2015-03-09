package adfs.client;

import java.io.IOException;
import java.io.InputStream;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;

import fuse.Filesystem3;
import fuse.FilesystemConstants;
import fuse.FuseDirFiller;
import fuse.FuseException;
import fuse.FuseGetattrSetter;
import fuse.FuseMount;
import fuse.FuseOpenSetter;
import fuse.FuseSizeSetter;
import fuse.FuseStatfsSetter;
import fuse.LifecycleSupport;
import fuse.XattrLister;
import fuse.XattrSupport;
import fuse.util.FuseArgumentParser;
import fuse4j.hadoopfs.FuseHdfsClient;
import adfs.core.ADFSActiveFileMeta;
import adfs.core.ADFSFile;
import adfs.core.ADFSFileMeta;
import adfs.core.ADFSFileContent;


public class ADFSClient implements Filesystem3, XattrSupport, LifecycleSupport {
	
	// --------- STRING CONSTANTS
	
	private static final String PROPERTIES_FILENAME = "ADFSClient.properties";
	private static final String PROP_CACHENAME = "infinispan_cache_name";
	private static final String PROP_FILESYSTEM = "filesystem";
	
	private static final String HDFS_S = "hdfs";
	private static final String PROP_HDFS_URL = "hdfs_url";
	
	
	// --------- VARIABLES
	// Log for this class, by default it searches for log4j.properties
    private static final Log log = LogFactory.getLog(ADFSClient.class);
    // Mount point for this filesystem
    // Useful for reading using java primitives activeconf files
    private String mountPoint;    
    // blabla
    private Filesystem3 fs;  
    // Infinispan cache to control the modifications on this filesystem
    private RemoteCache<String, ADFSFile> infinispanCache;
    
    
    // ADFS client javafs entry point
    public ADFSClient(String[] args) {
        this(new FuseArgumentParser(args).getMountPoint());
    }
    
    
    // ADFS client constructor
    public ADFSClient(String mountPoint) {
    	
    	this.mountPoint = mountPoint;
    	
    	Properties p = readPropertiesFile(PROPERTIES_FILENAME);    	
    	if(p == null) {
			log.error("property file not found in the classpath");
			System.exit(1);
    	}
    	
    	this.fs = buildFilesystem(p);    	
    	if(fs == null) {
			log.error("filesystem specified in properties file not supported yet");
			System.exit(1);
    	}
    	
    	// Other properties
    	String cacheName = p.getProperty(PROP_CACHENAME);
    	
    	// API entry point, by default it searches for hotrod-client.properties
    	RemoteCacheManager cacheContainer = new RemoteCacheManager();   	
    	// Obtain a handle to the remote default cache
    	this.infinispanCache = cacheContainer.getCache(cacheName);
    }
    
    
    // bla bla
    private Properties readPropertiesFile(String filename) {
		Properties configProp = new Properties();
		InputStream inputStream = getClass().getClassLoader().getResourceAsStream(filename);
		
		if(inputStream == null) return null;
		
		try {
			configProp.load(inputStream);
			return configProp;
		} catch (IOException e) {
			return null;
		}
    }
    
    
    // bla bla
    private Filesystem3 buildFilesystem(Properties p) {
		String fs = p.getProperty(PROP_FILESYSTEM);
		Filesystem3 fs3;
        
		if(fs.compareToIgnoreCase(HDFS_S) == 0) {
			String hdfs_url = p.getProperty(PROP_HDFS_URL);
			fs3 = new FuseHdfsClient(hdfs_url);
		}
		else
			fs3 = null;
		
		return fs3;
    } 
    
    
    
    // TODO: geattr: subs size and times??
    

    
    // For the ADFS, the mknod method needs to be modified.
    // Before we make a new node in the filesystem we need to announce 
    // to the infinispan cache that a new node will be created.
    // This node may be an active file, and the client will be responsible
    // for create the info stored in a configuration file associated to the
    // active file
    public int mknod(String path, int mode, int rdev) throws FuseException {
    	log.info("<adfs mknod> BEGIN-> PATH: " + path + " MODE: " + mode + " RDEV: " + rdev);
    	String pathMetaKey = ADFSFile.ATTR_PREFIX + path;
    	
    	// Use of the Cache to check if the path already exists.
    	//if(infinispanCache.containsKey(fileMetaKey)) {
    		//log.error("file: " + path + " already exists");
    		//throw new FuseException("chown not supported")
            //     .initErrno(FuseException.ENOSYS);
    		//throw new FuseException().initErrno(FuseException.EEXIST);
    		//return FuseException.EEXIST;
    	//}
    	
    	// Check if there is a config file for this new file
    	// and therefore mark this file as an active file (null value)
    	//String confMetaKey = ADFSFile.ATTR_PREFIX + path + ADFSFile.CONF_FILE_EXTENSION;
    	//ADFSFile confMeta = infinispanCache.get(confMetaKey);
    	//boolean activeFile = confMeta != null;
    	
    	// The object to store in the cache.
    	//ADFSFileMeta pathMeta = activeFile ?
    	//		null : new ADFSFileMeta(path);
    	
		// Send the object to the infinispan cache
    	// ATTENTION: MAY BLOCK FOR PROCESSING!!!
		// NOT ATOMIC FROM THE CONTAINS KEY OP!!!
    	// Solution: putIfAbsent with a transactional cache
    	
    	
    	// TODO check if the file exists
    	/*// Get the metadata for this file
    	ADFSFileMeta pathMeta = (ADFSFileMeta)infinispanCache.get(pathMetaKey);
    	log.info("<adfs open> RESULT-> infi get result: " + pathMeta);
    	
    	// If doesn't exists the client does the new put
    	if(pathMeta == null) {
    		infinispanCache.put(pathMetaKey, null);
    		pathMeta = (ADFSFileMeta)infinispanCache.get(pathMetaKey);
    		log.info("<adfs open> RESULT-> infi get was null, after put: " + pathMeta);
    	}*/
    	
    	
    	ADFSFile pathOldMeta = infinispanCache.put(pathMetaKey, null);
    	log.info("<adfs mknod> RESULT-> infi put result: " + pathOldMeta);

    	// Create the file in the fs
    	int ret = fs.mknod(path, mode, rdev);
    	if(ret != 0) {
    		ADFSFile retMeta = infinispanCache.remove(pathMetaKey);
    		ADFSFile retCont = infinispanCache.remove(path);
    		
    		throw new FuseException("<adfs mknod> ERROR-> fs error" +
    				", remove result Meta: " + retMeta +
    				", remove result Content: " + retCont).initErrno(ret);
	    }

    	return 0;
    }
    
    
    // blabla
    public int mkdir(String path, int mode) throws FuseException {  	
    	log.info("<adfs mkdir> BEGIN-> PATH: " + path + " MODE: " + mode);
    	String pathMetaKey = ADFSFile.ATTR_PREFIX + path;    	
    	
    	// Register the directory in infinispan
    	ADFSFile pathOldMeta = infinispanCache.put(pathMetaKey, null);
    	log.info("<adfs mkdir> RESULT-> infi put result: " + pathOldMeta);
    	
    	// Create the dir in the fs
    	int ret = fs.mkdir(path, mode);
    	if(ret != 0) {
    		ADFSFile retMeta = infinispanCache.remove(pathMetaKey);
    		ADFSFile retCont = infinispanCache.remove(path);
    		
    		throw new FuseException("<adfs mkdir> ERROR-> fs error" +
    				", remove result Meta: " + retMeta +
    				", remove result Content: " + retCont).initErrno(ret);
    	}
    	
    	return 0;
    }
    
    
    // blabla
    public int unlink(String path) throws FuseException {
    	log.info("<adfs unlink> BEGIN-> PATH: " + path);
    	String pathMetaKey = ADFSFile.ATTR_PREFIX + path;
    	
    	//BLABLA
    	ADFSFileMeta pathMeta = (ADFSFileMeta)infinispanCache.get(pathMetaKey);
    	if(pathMeta != null && !pathMeta.isAvailable())
    		throw new FuseException("<adfs unlink> ERROR-> file not available")
    			.initErrno(FuseException.EPERM);
    	
    	// Remove the file in infinispan
    	ADFSFile pathOldMeta = infinispanCache.remove(pathMetaKey);
    	ADFSFile pathOldContent = infinispanCache.remove(path);
    	log.info("<adfs unlink> RESULT-> infi remove result: " + pathOldMeta);
    	
    	// Remove the path in the fs
    	int ret = fs.unlink(path);
    	if(ret != 0) {
    		
    		if(pathOldMeta != null) {
    			infinispanCache.put(pathMetaKey, pathOldMeta);
    			infinispanCache.put(path, pathOldContent);
    		}
    		
    		throw new FuseException("<adfs unlink> ERROR-> fs error")
    			.initErrno(ret);
    	}
    	
    	return 0;
    }
    
    
    // blabla
    public int rmdir(String path) throws FuseException {
    	log.info("<adfs rmdir> BEGIN-> PATH: " + path);
    	String pathMetaKey = ADFSFile.ATTR_PREFIX + path;
    	
    	// TODO available?
    	
    	// Remove the dir in infinispan
    	ADFSFile pathOldMeta = infinispanCache.remove(pathMetaKey);
    	ADFSFile pathOldContent = infinispanCache.remove(path);
    	log.info("<adfs rmdir> RESULT-> infi remove result: " + pathOldMeta);
    	
    	// Remove the dir in the fs
    	int ret = fs.rmdir(path);
    	if(ret != 0) {
    		
    		if(pathOldMeta != null) {
    			infinispanCache.put(pathMetaKey, pathOldMeta);
    			infinispanCache.put(path, pathOldContent);
    		}
    		
    		log.info("<adfs rmdir> ERROR-> fs error " + ret);
    		return ret;
    	}
    	
    	return 0;
    }
    
/*    private List<String> getAllFilesAndSubdirs(String path) {
    	List<String> l = new LinkedList<String>();
    	
    	File f = new File(path);
    	
    	for(File sf: f.listFiles())
    		if(sf.isDirectory())
    			l.addAll(getAllFilesAndSubdirs(sf.getPath()));
    		else
    			l.add(sf.getPath());
    	
    	log.info(l);
    	return l;
    }*/
    
    
    // blabla
    public int rename(String from, String to) throws FuseException {
    	log.info("<adfs rename> BEGIN-> FROM: " + from + " TO: " + to);
    	String fromMetaKey = ADFSFile.ATTR_PREFIX + from;   
    	String toMetaKey = ADFSFile.ATTR_PREFIX + to;
    	
    	// TODO available?
    	
    	// Replace the entry in infinispan, MUST BE ATOMIC AND IS NOT
		ADFSFile fromMeta = infinispanCache.remove(fromMetaKey);
		ADFSFile fromContent = infinispanCache.remove(from);
		
		// TODO Change name
		
    	ADFSFile toOldMeta = infinispanCache.put(toMetaKey, fromMeta);
    	ADFSFile toOldContent = infinispanCache.put(to, fromContent);
    	
    	log.info("<adfs rename> RESULT-> infi remove from: " + fromMeta + 
    			", infi put toOld: " + toOldMeta);
    	
    	// Rename the node in the fs
    	int ret = fs.rename(from, to);
    	if(ret != 0) {
    		ADFSFile toMetaErr = infinispanCache.remove(toMetaKey);
    		ADFSFile toContentErr = infinispanCache.remove(to);
    		// TODO check if 'from' exists
        	ADFSFile fromOldMetaErr = infinispanCache.put(fromMetaKey, toMetaErr);
        	ADFSFile fromOldContentErr = infinispanCache.put(from, toContentErr);
        	
        	log.info("<adfs rename> ERROR-> fs error infi remove to: " + toMetaErr + 
        			", infi put fromOld: " + fromOldMetaErr);
    		return ret;
    	}
    	
    	return 0;
    }
    
    
    // blabla
    public int open(String path, int flags, FuseOpenSetter openSetter) throws FuseException {
    	log.info("<adfs open> BEGIN-> PATH: " + path + " FLAGS: " + flags);
    	String pathMetaKey = ADFSFile.ATTR_PREFIX + path;
    	
    	// Get the metadata for this file
    	ADFSFileMeta pathMeta = (ADFSFileMeta)infinispanCache.get(pathMetaKey);
    	log.info("<adfs open> RESULT-> infi get result: " + pathMeta);
    	
    	// If doesn't exists the client does the new put
    	if(pathMeta == null) {
    		infinispanCache.put(pathMetaKey, null);
    		pathMeta = (ADFSFileMeta)infinispanCache.get(pathMetaKey);
    		log.info("<adfs open> RESULT-> infi get was null, after put: " + pathMeta);
    	}
    	
    	if(pathMeta == null)
    		throw new FuseException().initErrno(FuseException.ENOENT);
    	
    	if(pathMeta.isActive() && !isReadOnlyAccess(flags)) {
    		log.info("<adfs open> not allowed modify an active file");
    		throw new FuseException("not allowed to modify an active file")
    				.initErrno(FuseException.EPERM);
    	}
    	
    	//BLABLA
    	if(pathMeta.isActive() && !pathMeta.isAvailable()) // TODO message
    		throw new FuseException().initErrno(FuseException.EPERM);
    	
    	if(!pathMeta.isAvailable() && !isReadOnlyAccess(flags)) // TODO message
    		throw new FuseException().initErrno(FuseException.EPERM);
    	
    	// Let infinispan now that we need to open the file
    	// If the processing is lazy, this op may be blocked
    	// != null because meta != null
    	ADFSFileContent pathContent = (ADFSFileContent)infinispanCache.get(path);
    	
    	// A computation was launched
    	if(pathContent == null)
    		throw new FuseException("Active file started computing")
    			.initErrno(FuseException.EPERM);
    	
    	if(pathMeta.isActive() &&
    			((ADFSActiveFileMeta)pathMeta).isStale()) // TODO message
    		throw new FuseException().initErrno(FuseException.EPERM);
    	
    	
    	if(pathContent.getData() != null)
			log.info("<adfs open> INFO-> FILE IN CACHE: " + path +
					" SIZE: " + pathContent.getData().length);
    	
    	if(isReadOnlyAccess(flags)) {
    		log.info("<adfs open> INFO-> READ ONLY ACCESS: " + path);
        	// If the file is in the cache we can
    		// read the contents directly
        	if(pathContent.getData() != null) {
        		openSetter.setFh(new ADFSFileHandler(pathContent, null));
        		return 0;
        	}
        	else
        		return fs.open(path, flags, openSetter);
    	}
    	else {
    		log.info("<adfs open> INFO-> NOT READ ONLY ACCESS: " + path);
        	// If the file is registered in infinispan
    		// both filehandlers from adfs and fileystem need to be passed
    		
    		int ret = fs.open(path, flags, openSetter);        		
    		if(ret != 0) return ret;
   		
    		Object fhFS = openSetter.getFh();
    		openSetter.setFh(new ADFSFileHandler(pathContent, fhFS));
    		return 0;
    	}
    }
    
    
    // blabla
    public int read(String path, Object fh, ByteBuffer buf, long offset) throws FuseException {
    	log.info("<adfs read> BEGIN-> PATH: " + path + " SIZE: " + buf.capacity() +
    			" OFFSET: " + offset);
    	
    	if(fh instanceof ADFSFileHandler) {  // content is in cache
    		log.info("<adfs read> INFO-> read from cache");
    		byte[] cacheData = ((ADFSFileHandler)fh).getContent().getData();
    		byte[] fuseData = new byte[buf.capacity()];
    		
    		if(cacheData.length < fuseData.length)
    			System.arraycopy(cacheData, (int)offset, fuseData, 0, cacheData.length);
    		else
    			System.arraycopy(cacheData, (int)offset, fuseData, 0, fuseData.length);
    		
    		buf.put(fuseData);
    		
    		return 0;
    	}
    	else {
    		log.info("<adfs read> INFO-> read from FS");
    		return fs.read(path, fh, buf, offset);
    	}
    }
    
    
    // blabl
    public int write(String path, Object fh, boolean isWritepage, ByteBuffer buf, long offset) throws FuseException {
    	log.info("<adfs write> BEGIN-> PATH: " + path + " SIZE: " + buf.capacity() +
    			" OFFSET: " + offset);
		
		ADFSFileHandler fhADFS = (ADFSFileHandler)fh;
		//ADFSFileMeta pathMeta = fhADFS.getMeta();
		ADFSFileContent pathContent = fhADFS.getContent();
		
		if(pathContent.getData() != null) {
			log.info("<adfs write> INFO-> write in cache");
			
			byte[] cacheData = pathContent.getData();		
			
    		if(offset + buf.capacity() <= pathContent.getMaxSize()) {
    			log.info("<adfs write> INFO-> write < size");
    			
    			byte[] caheNewData;
    			if((offset + buf.capacity()) > cacheData.length) {
    				caheNewData = new byte[(int)(offset + buf.capacity())];
    				System.arraycopy(cacheData, 0, caheNewData, 0, cacheData.length);
    			}
    			else
    				caheNewData = cacheData;
    			
    	        // get the data to write
    	        byte[] writeBuf = new byte[buf.capacity()];
    	        buf.get(writeBuf, 0, writeBuf.length);

    			System.arraycopy(writeBuf, 0, caheNewData, (int)offset, writeBuf.length);
    			pathContent.setData(caheNewData);
    			
    			//buf.position(writeBuf.length);
    			buf.rewind(); // for the FS write
    		}
    		else {
    			log.info("<adfs write> INFO-> write > size");
    			pathContent.setData(null);
    		}
		}
		
		//We have to propagate the changes to the filesystem
		log.info("<adfs write> INFO-> write to the FS");
		int ret = fs.write(path, fhADFS.getFhFileSystem(), isWritepage, buf, offset);
		if(ret != 0) {
			// TODO
			return ret;
		}

		fhADFS.setModified();
		return 0;
    }
    
    
    // blabla
    public int release(String path, Object fh, int flags) throws FuseException {
    	log.info("<adfs release> BEGIN-> PATH: " + path + " FLAGS: " + flags);
    	
    	if(fh instanceof ADFSFileHandler) {
    		log.info("<adfs release> INFO-> release ADFSFileHandler");
    		
    		ADFSFileHandler fhADFS = (ADFSFileHandler)fh;
    		//ADFSFileMeta pathMeta = fhADFS.getMeta();
    		ADFSFileContent pathContent = fhADFS.getContent();
    		
    		// Only if the file was modified, we need to let know infinispan
    		// otherwise there is nothing to do
    		if(fhADFS.isModified()) {
    			log.info("<adfs release> INFO-> content was modified, telling infinispan...");
				
    			// The content was modified so we need to notify the infinispan
    			// even with null content
    			infinispanCache.put(path, pathContent);
    		}
    		
    		// Writes with files registered in infinispan were made in
    		// the filesystem and we need to realease them
    		if(fhADFS.getFhFileSystem() != null) {
    			log.info("<adfs release> INFO-> release in FS");
    			int ret = fs.release(path, fhADFS.getFhFileSystem(), flags);
    			if(ret != 0) {
    				// TODO
    				return ret;
    			}
    		}
    		
    		return 0;
    	}
    	else
    		return fs.release(path, fh, flags);
    }
    
    //blabla n e preciso modificar...
	public int getattr(String path, FuseGetattrSetter getattrSetter)
			throws FuseException {
		return fs.getattr(path, getattrSetter);
	}


	public int readlink(String path, CharBuffer link)
			throws FuseException {
		return fs.readlink(path, link);
	}


	public int getdir(String path, FuseDirFiller dirFiller)
			throws FuseException {
		return fs.getdir(path, dirFiller);
	}


	public int symlink(String from, String to)
			throws FuseException {
		return fs.symlink(from, to);
	}


	public int link(String from, String to)
			throws FuseException {
		return fs.link(from, to);
	}


	public int chmod(String path, int mode)
			throws FuseException {
		return fs.chmod(path, mode);
	}


	public int chown(String path, int uid, int gid)
			throws FuseException {
		return fs.chown(path, uid, gid);
	}


	public int truncate(String path, long size)
			throws FuseException {
		return fs.truncate(path, size);
	}


	public int utime(String path, int atime, int mtime)
			throws FuseException {
		return fs.utime(path, atime, mtime);
	}


	public int statfs(FuseStatfsSetter statfsSetter)
			throws FuseException {
		return fs.statfs(statfsSetter);
	}


	public int flush(String path, Object fh)
			throws FuseException {
		return fs.flush(path, fh);
	}


	public int fsync(String path, Object fh, boolean isDatasync)
			throws FuseException {
		return fs.fsync(path, fh, isDatasync);
	}
	
	// XattrSupport
	
	public int getxattrsize(String path, String name, FuseSizeSetter sizeSetter)
			throws FuseException {
		if(fs instanceof XattrSupport)
			return ((XattrSupport)fs).getxattrsize(path, name, sizeSetter);
		else
			return 0;
	}

	
	public int getxattr(String path, String name, ByteBuffer dst, int position)
			throws FuseException, BufferOverflowException {
		if(fs instanceof XattrSupport)
			return ((XattrSupport)fs).getxattr(path, name, dst, position);
		else
			return 0;
	}


	public int listxattr(String path, XattrLister lister) throws FuseException {
		if(fs instanceof XattrSupport)
			return ((XattrSupport)fs).listxattr(path, lister);
		else
			return 0;
	}


	public int setxattr(String path, String name, ByteBuffer value, int flags,
			int position) throws FuseException {
		if(fs instanceof XattrSupport)
			return ((XattrSupport)fs).setxattr(path, name, value, flags, position);
		else
			return 0;
	}


	public int removexattr(String path, String name) throws FuseException {
		if(fs instanceof XattrSupport)
			return ((XattrSupport)fs).removexattr(path, name);
		else
			return 0;
	}
	
	// LifecycleSupport
	
	public int init() {
		if(fs instanceof LifecycleSupport)
			return ((LifecycleSupport)fs).init();
		else
			return 0;
	}


	public int destroy() {
		// End the conection with infinispan
		this.infinispanCache.stop();
		
		if(fs instanceof LifecycleSupport)
			return ((LifecycleSupport)fs).destroy();
		else
			return 0;
	}
    
    
    //
    // Java entry point
    public static void main(String[] args) {
        log.info("entering");
		
        try {
        	String mountPoint = new FuseArgumentParser(args).getMountPoint();
            FuseMount.mount(args, new ADFSClient(mountPoint), log);
        }
        catch(Exception e) {
            e.printStackTrace();
        }
        finally {
            log.info("exiting");
        }
    }
    
    
    /**
     * isReadOnlyAccess()
     */
    private boolean isReadOnlyAccess(int flags) {
        return ((flags & 0x0FF) == FilesystemConstants.O_RDONLY);
    }
    
    
    // blabla
    private class ADFSFileHandler {
    	
    	private ADFSFileContent content;
    	private Object fhFileSystem;
    	private boolean modified;
    	
    	public ADFSFileHandler(ADFSFileContent content, Object fhFileSystem) {
    		this.content = content;
    		this.fhFileSystem = fhFileSystem;
    		this.modified = false;
    	}
    	
    	public ADFSFileContent getContent() { return content; }
    	public Object getFhFileSystem()     { return fhFileSystem; }
    	
    	public void setModified()           { modified = true; }
    	public boolean isModified()         { return modified; }
    }

}
