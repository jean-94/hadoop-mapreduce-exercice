package com.opstty.mapper;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;

import static java.lang.Double.NaN;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class SortHeightMapperTest {
    @Mock
    private Mapper.Context context;
    private SortHeightMapper SortHeightMapper;

    @Before
    public void setup() {
        this.SortHeightMapper = new SortHeightMapper();
    }

    @Test
    public void testMap() throws IOException, InterruptedException {
        String l1 = "GEOPOINT;ARRONDISSEMENT;GENRE;ESPECE;FAMILLE;ANNEE PLANTATION;HAUTEUR;CIRCONFERENCE;ADRESSE;NOM COMMUN;VARIETE;OBJECTID;NOM_EV";
        String l2 = "(48.857140829, 2.29533455314);7;Maclura;pomifera;Moraceae;1935;13.0;;Quai Branly, avenue de La Motte-Piquet, avenue de la Bourdonnais, avenue de Suffren;Oranger des Osages;;6;Parc du Champs de Mars\n";
        Text word = new Text(" ");
        this.SortHeightMapper.map(null, new Text(l1), this.context);
        verify(this.context, times(0)).write(word, new DoubleWritable(NaN));
        this.SortHeightMapper.map(null, new Text(l2), this.context);
        verify(this.context, times(1)).write(word, new DoubleWritable(13.0));
    }
}
