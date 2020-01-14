def file = new File(request.getOutputDirectory(), request.getArtifactId() + "/ES_IndexInit.sh")
file.setExecutable(true, false)