def file = new File(request.getOutputDirectory(), request.getArtifactId() + "/ES_IndexInit.sh")
file.setExecutable(true, false)

def file2 = new File(request.getOutputDirectory(), request.getArtifactId() + "/kibana/importKibana.sh")
file2.setExecutable(true, false)
