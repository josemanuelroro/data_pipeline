
import java.util.logging.*
import jenkins.model.*
import org.kohsuke.stapler.Stapler
import java.text.MessageFormat


def loggerName = "jenkins.security.SecurityListener"
def targetLogger = Logger.getLogger(loggerName)
String logPath = "/data_shared/logs/security-audit.log"

class SmartFormatter extends Formatter {
    String format(LogRecord record) {
        String ip = "System/Internal"
        String msg = record.getMessage()
        
        
        if (record.getParameters() != null && record.getParameters().length > 0) {
            try {
                msg = MessageFormat.format(msg, record.getParameters())
            } catch (Exception e) {
                
            }
        }

        
        try {
            def req = Stapler.getCurrentRequest()
            if (req != null) {
                String forwarded = req.getHeader("X-Forwarded-For")
                if (forwarded != null) {
                    ip = forwarded.split(",")[0].trim()
                } else {
                    ip = req.getRemoteAddr()
                }
            }
        } catch (Exception e) { ip = "Unknown" }

        
        return new Date(record.getMillis()).format("yyyy-MM-dd HH:mm:ss") + " | " + ip + " | " + msg + "\n"
    }
}


try {
    
    File folder = new File("/data_shared/logs")
    if (!folder.exists()) folder.mkdirs()

    
    if (!folder.canWrite()) {
        println "ERROR"
        return
    }

    
    for (Handler h : targetLogger.getHandlers()) {
        if (h instanceof FileHandler) targetLogger.removeHandler(h)
    }

    
    FileHandler handler = new FileHandler(logPath, 1024 * 1024 * 10, 3, true)
    handler.setFormatter(new SmartFormatter())
    handler.setLevel(Level.ALL)

    targetLogger.addHandler(handler)
    targetLogger.setLevel(Level.ALL)
    
    println "LOGGER ACTIVE" + logPath

} catch (Exception e) {
    println "ERROR" + e.getMessage()
    e.printStackTrace()
}
