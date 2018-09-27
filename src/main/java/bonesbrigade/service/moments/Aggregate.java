package bonesbrigade.service.moments;

public class Aggregate {

    final int count;
    final double mean;
    final double squaredDistance;

    public Aggregate(int count, double mean, double squaredDistance) {

        this.count = count;
        this.mean = mean;
        this.squaredDistance = squaredDistance;
    }

    public Aggregate() {
        this(0, 0.0, 0.0);
    }

    @Override
    public String toString() {
        return "Aggregate{" +
                "count=" + count +
                ", mean=" + mean +
                ", squaredDistance=" + squaredDistance +
                '}';
    }
}
