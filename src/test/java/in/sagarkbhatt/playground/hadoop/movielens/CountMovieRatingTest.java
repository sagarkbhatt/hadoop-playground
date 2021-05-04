package in.sagarkbhatt.playground.hadoop.movielens;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.Arrays;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@ExtendWith(MockitoExtension.class)
class CountMovieRatingTest {

    @Mock
    private Mapper.Context mockMapperContext;

    @Mock
    private Reducer.Context mockReducerContext;

    private CountMovieRating.IntSumReducer reducer;

    private CountMovieRating.TokenizerMapper mapper;

    @BeforeEach
    public void setUp() throws Exception {
        mapper = new CountMovieRating.TokenizerMapper();
        reducer = new CountMovieRating.IntSumReducer();
    }

    @Test
    void shouldMapDataToKeyValuePair() throws IOException, InterruptedException {
        String line1 = "186\t302\t3\t891717742";
        IntWritable one = new IntWritable(1);

        mapper.map(1, new Text(line1), mockMapperContext);

        verify(mockMapperContext).write(new Text("3"), one);
    }

    @Test
    void shouldReduceData() throws IOException, InterruptedException {
        IntWritable one1 = new IntWritable(1);
        IntWritable one2 = new IntWritable(1);
        IntWritable one3 = new IntWritable(1);
        Text key = new Text("3");

        Iterable<IntWritable> iterable = Arrays.asList(one1, one2, one3);

        reducer.reduce(key, iterable, mockReducerContext);

        verify(mockReducerContext).write(key, new IntWritable(3));
    }

    @AfterEach
    public void tearDown() {
        verifyNoMoreInteractions(mockMapperContext);
        verifyNoMoreInteractions(mockReducerContext);
    }
}