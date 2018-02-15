import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.mapreduce.lib.db.DBWritable;

public class DBOutputWritable implements DBWritable{

    private String userID;
    private String movieID;
    private double rating;

    public DBOutputWritable(String userID, String movieID, double rating) {
        this.userID = userID;
        this.movieID = movieID;
        this.rating = rating;
    }

    public void readFields(ResultSet arg0) throws SQLException {
        this.userID = arg0.getString(1);
        this.movieID = arg0.getString(2);
        this.rating = arg0.getDouble(3);
    }

    public void write(PreparedStatement arg0) throws SQLException {
        arg0.setString(1, userID);
        arg0.setString(2, movieID);
        arg0.setDouble(3, rating);
    }

}
