def file1 = new File(request.getOutputDirectory(), request.getArtifactId() + "/dashboards/importDashboards.sh")
file1.setExecutable(true, false)

def file2 = new File(request.getOutputDirectory(), request.getArtifactId() + "/OS_IndexInit.sh")
file2.setExecutable(true, false)
