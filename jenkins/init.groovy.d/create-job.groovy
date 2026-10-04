// Crea el job del pipeline al arrancar Jenkins (job-as-code, sin pasos manuales).
// Se copia desde /usr/share/jenkins/ref/init.groovy.d solo si no existe en JENKINS_HOME.
import jenkins.model.Jenkins
import hudson.plugins.git.GitSCM
import hudson.plugins.git.BranchSpec
import org.jenkinsci.plugins.workflow.cps.CpsScmFlowDefinition
import org.jenkinsci.plugins.workflow.job.WorkflowJob
import com.cloudbees.jenkins.GitHubPushTrigger

def repoUrl = 'https://github.com/elrios893/pokemon-geo-bigdata.git'
def jobName = 'pokemon-geo-bigdata'
def jenkins = Jenkins.get()

if (jenkins.getItem(jobName) == null) {
    def job = jenkins.createProject(WorkflowJob, jobName)
    def scm = new GitSCM(GitSCM.createRepoList(repoUrl, null),
                         [new BranchSpec('*/main')], null, null, [])
    job.setDefinition(new CpsScmFlowDefinition(scm, 'Jenkinsfile'))
    job.addTrigger(new GitHubPushTrigger())
    job.save()
    println "init: job '${jobName}' creado"
} else {
    println "init: job '${jobName}' ya existe"
}
