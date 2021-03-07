package org.example.batch;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;

public class StudentSubjectScores {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple4<String, String, Double, Double>> studentScores = env
                .readCsvFile(Constants.STUDENT_SCORES_FILE_INPUT)
                .ignoreFirstLine()
                .parseQuotedStrings('\"')
                .types(String.class, String.class, Double.class, Double.class);
        //studentScores.print();
        //total score for each student + subject
        DataSet<Tuple3<String, String, Double>> studentSubjectTotalScore = studentScores
                .map(new MapFunction<Tuple4<String, String, Double, Double>, Tuple3<String, String, Double>>() {
                    @Override
                    public Tuple3<String, String, Double> map(Tuple4<String, String, Double, Double> value) throws Exception {
                        return new Tuple3<>(value.f0, value.f1, value.f2 + value.f3);
                    }
                });
        studentSubjectTotalScore.print();
        //Total score for each student in Physics
        studentSubjectTotalScore.filter(new FilterFunction<Tuple3<String, String, Double>>() {
            @Override
            public boolean filter(Tuple3<String, String, Double> value) throws Exception {
                return value.f1.equals("Physics");
            }
        }).print();
        //average total score for each student across all subjects
        DataSet<Tuple2<String, Double>> studentTotalScores = studentSubjectTotalScore
                .map(new MapFunction<Tuple3<String, String, Double>, Tuple2<String, Double>>() {
                    @Override
                    public Tuple2<String, Double> map(Tuple3<String, String, Double> value) throws Exception {
                        return new Tuple2<>(value.f0, value.f2);
                    }
                }).groupBy(0).aggregate(Aggregations.SUM, 1);
        DataSet<Tuple2<String, Integer>> numberOfSubjectsPerStudent = studentScores.map(new MapFunction<Tuple4<String, String, Double, Double>, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(Tuple4<String, String, Double, Double> value) throws Exception {
                return new Tuple2<>(value.f0, 1);
            }
        }).groupBy(0).sum(1);
        DataSet<Tuple2<String, Double>> averageTotalScorePerStudent = studentTotalScores
                .join(numberOfSubjectsPerStudent)
                .where(0)
                .equalTo(0)
                .with(new JoinFunction<Tuple2<String, Double>, Tuple2<String, Integer>, Tuple2<String, Double>>() {
                    @Override
                    public Tuple2<String, Double> join(Tuple2<String, Double> totalScore, Tuple2<String, Integer> numberOfSubjects) throws Exception {
                        return new Tuple2<>(totalScore.f0, totalScore.f1 / numberOfSubjects.f1);
                    }
                });
        averageTotalScorePerStudent.print();
        //student with highest total score for each subject
        studentSubjectTotalScore.groupBy(1).reduce(new ReduceFunction<Tuple3<String, String, Double>>() {
            @Override
            public Tuple3<String, String, Double> reduce(Tuple3<String, String, Double> value1, Tuple3<String, String, Double> value2) throws Exception {
                return value1.f2>value2.f2?value1:value2;
            }
        }).print();
    }
}
