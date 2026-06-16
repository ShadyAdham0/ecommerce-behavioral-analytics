import os

def ensure_hadoop_home():
    if os.name != 'nt':
        return

    current_dir = os.path.dirname(os.path.abspath(__file__))
    hadoop_home = os.path.join(current_dir, "hadoop_win")
    
    if os.path.isdir(hadoop_home):
        os.environ["HADOOP_HOME"] = hadoop_home
        hadoop_bin = os.path.join(hadoop_home, "bin")
        
        path_env = os.environ.get("PATH", "")
        if hadoop_bin not in path_env:
            os.environ["PATH"] = hadoop_bin + os.pathsep + path_env
