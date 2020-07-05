import CasConta.MapperCases;
import CasConta.ReducerCases;
import CasMort.MapperMort;
import CasMort.ReducerMort;
import RatioDeCadaLina.MapperLine;
import RationConPais.MapperRatioPais;
import RationConPais.ReducerRatioPais;
import Top10Ratio.TopMapRatio;
import Top10Ratio.TopReducerRatio;
import TopNCas.TopNCasMapper;
import TopNCas.TopNCasReducer;
import TopNM.TopnNMortMapper;
import TopNM.TopnNMortReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;

public class FinalMain extends Configured implements Tool {



    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        String[] args1 = new GenericOptionsParser(conf, args).getRemainingArgs();
        FileSystem.get(conf).delete(new Path("src/main/resources/uno"), true);
        FileSystem.get(conf).delete(new Path("src/main/resources/two"), true);
        FileSystem.get(conf).delete(new Path("src/main/resources/three"), true);
        FileSystem.get(conf).delete(new Path("src/main/resources/fourth"), true);
        FileSystem.get(conf).delete(new Path("src/main/resources/five"), true);
        FileSystem.get(conf).delete(new Path("src/main/resources/six"), true);
        FileSystem.get(conf).delete(new Path("src/main/resources/seven"), true);
        FileSystem.get(conf).delete(new Path("src/main/resources/output"), true);

        // ici je presente le nombre de morts de chaque pays des la date du 27 selon le fichier csv
        Job job1 = Job.getInstance(conf);
        job1.setJobName("mort par pays");
        job1.setJarByClass(FinalMain.class);

        job1.setMapperClass(MapperMort.class);
        job1.setCombinerClass(ReducerMort.class);
        job1.setReducerClass(ReducerMort.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        job1.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path("src/main/resources/uno"));

        ControlledJob cJob1 = new ControlledJob(conf);
        cJob1.setJob(job1);


        JobControl jobctrl = new JobControl("JobCtrl");
        jobctrl.addJob(cJob1);


        //++++++second job
        // ici j'ordonne le nombre de nombre de mort a ordre decroissant
        Job job2 = Job.getInstance(conf);
        job2.setJobName("topN");
        job2.setJarByClass(FinalMain.class);

        job2.setMapperClass(TopnNMortMapper.class);
        job2.setReducerClass(TopnNMortReducer.class);
        job2.setNumReduceTasks(1);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path("src/main/resources/uno"));
        FileOutputFormat.setOutputPath(job2, new Path("src/main/resources/two"));

        ControlledJob cJob2 = new ControlledJob(conf);
        cJob2.setJob(job2);

        jobctrl.addJob(cJob1);
        jobctrl.addJob(cJob2);
        cJob2.addDependingJob(cJob1);

        //+++++++++++++++++++++++
        // je recupere le nombre de cas de chaque pays
        Job job3 = Job.getInstance(conf);
        job3.setJobName("Contaminé");
        job3.setJarByClass(FinalMain.class);

        job3.setMapperClass(MapperCases.class);
        job3.setReducerClass(ReducerCases.class);
        job3.setNumReduceTasks(1);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job3, new Path(args[0]));
        FileOutputFormat.setOutputPath(job3, new Path("src/main/resources/three"));

        ControlledJob cJob3 = new ControlledJob(conf);
        cJob3.setJob(job3);

        jobctrl.addJob(cJob1);
        jobctrl.addJob(cJob2);
        jobctrl.addJob(cJob3);



        //+++++++++++++++++++++++++
        // je classe le nombre de cas selo leur ordre(decroissant)

        Job job4 = Job.getInstance(conf);
        job4.setJobName("Top Cas");
        job4.setJarByClass(FinalMain.class);

        job4.setMapperClass(TopNCasMapper.class);
        job4.setReducerClass(TopNCasReducer.class);
        job4.setNumReduceTasks(1);
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job4, new Path("src/main/resources/three"));
        FileOutputFormat.setOutputPath(job4, new Path("src/main/resources/fourth"));

        ControlledJob cJob4 = new ControlledJob(conf);
        cJob4.setJob(job4);


        jobctrl.addJob(cJob1);
        jobctrl.addJob(cJob2);
        jobctrl.addJob(cJob3);
        jobctrl.addJob(cJob4);
        cJob4.addDependingJob(cJob3);

        //+++++ ratio de cada pays
        // ici je calcul le ratio de chaque ligne ou de chaque objet nbre cas/nbre morts

        Job job5 = Job.getInstance(conf);
        job5.setJobName("Ratio");
        job5.setJarByClass(FinalMain.class);

        job5.setMapperClass(MapperLine.class);
        //job5.setReducerClass(ReducerCases.class);
        //job5.setNumReduceTasks(1);
        //recevoir Text
        job5.setOutputKeyClass(Text.class);
        job5.setOutputValueClass(FloatWritable.class);
        FileInputFormat.addInputPath(job5, new Path(args[0]));
        FileOutputFormat.setOutputPath(job5, new Path("src/main/resources/five"));

        ControlledJob cJob5 = new ControlledJob(conf);
        cJob5.setJob(job5);

        jobctrl.addJob(cJob1);
        jobctrl.addJob(cJob2);
        jobctrl.addJob(cJob3);
        jobctrl.addJob(cJob4);
        jobctrl.addJob(cJob5);


        //++++++++++++++++++++
        // je regroupe tous les cas en fonctions des en addtitionnant les ratio pour chaque pays
        Job job6 = Job.getInstance(conf);
        job6.setJobName("Mapper Ratio Pais");
        job6.setJarByClass(FinalMain.class);

        job6.setMapperClass(MapperRatioPais.class);
        job6.setReducerClass(ReducerRatioPais.class);
        job6.setNumReduceTasks(1);
        job6.setOutputKeyClass(Text.class);
        job6.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job6, new Path("src/main/resources/five"));
        FileOutputFormat.setOutputPath(job6, new Path("src/main/resources/six"));

        ControlledJob cJob6 = new ControlledJob(conf);
        cJob6.setJob(job6);


        jobctrl.addJob(cJob1);
        jobctrl.addJob(cJob2);
        jobctrl.addJob(cJob3);
        jobctrl.addJob(cJob4);
        jobctrl.addJob(cJob5);
        jobctrl.addJob(cJob6);
        cJob6.addDependingJob(cJob5);

        //++++++++++++++++++++
        // je les affiche donc par odre decroissant
        Job job7 = Job.getInstance(conf);
        job7.setJobName("Top Cas");
        job7.setJarByClass(FinalMain.class);

        job7.setMapperClass(TopMapRatio.class);
        job7.setReducerClass(TopReducerRatio.class);
        job7.setNumReduceTasks(1);
        job7.setOutputKeyClass(Text.class);
        job7.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job7, new Path("src/main/resources/six"));
        FileOutputFormat.setOutputPath(job7, new Path("src/main/resources/seven"));

        ControlledJob cJob7 = new ControlledJob(conf);
        cJob7.setJob(job7);


        jobctrl.addJob(cJob1);
        jobctrl.addJob(cJob2);
        jobctrl.addJob(cJob3);
        jobctrl.addJob(cJob4);
        jobctrl.addJob(cJob5);
        jobctrl.addJob(cJob6);
        jobctrl.addJob(cJob7);
        cJob7.addDependingJob(cJob6);


        Thread jobRunnerThread = new Thread(new JobRunner(jobctrl));
        jobRunnerThread.start();
        while (!jobctrl.allFinished()) {
            System.out.println("Still running...");
            Thread.sleep(5000);
        }
        System.out.println("done");
        jobctrl.stop();
        return(0);
    }

    private class JobRunner implements Runnable {

        private JobControl control;
        public JobRunner(JobControl jobctrl) {
            this.control = jobctrl;
        }

        @Override
        public void run() {
            this.control.run();
        }

    }

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        int exitCode = ToolRunner.run(new FinalMain(), args);
        System.exit(exitCode);
    }
}
