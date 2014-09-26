///*
// *
// * @author Ke Wang Since June 2011
// */
//
///*
// * this class is the canvas for painting
// */
//
//import javax.swing.*;
//import java.awt.*;
//
public class ModCanvas {
	
}
//public class ModCanvas extends JPanel
//{
//    private int size;
//    private Color[][] nodesColor;
//    private SimMatrix ds;
//    private int pixSize;
//    private int maxLoad;
//    private int minLoad;
//    private int loadInterval;
//    private int windowSize;
//    
//    public ModCanvas(int size, SimMatrix ds)
//    {
//        this.size = size;
//        this.ds = ds;
//        maxLoad = (int)Library.numTaskToSubmit;
//        minLoad = -Library.numCorePerNode;
//        loadInterval = maxLoad - minLoad;
//        nodesColor = new Color[size][size];
//        windowSize = 750;
//        pixSize = windowSize / size;
//    }
//    
//    public int getWindowSize()
//    {
//        return windowSize;
//    }
//
//    public Color load2Color(int load, int x, int y)
//    {
//        Color fromLoad;
//        float rate = (load - this.minLoad) / (float)(this.loadInterval);
//        if (rate > 1.0 || rate < 0.0)
//        {
//            System.out.println("Bug: " + load);
//        }
//        //Color fromLoad = new Color((int)(rate*255), (int)((1-rate)*255), 0);
//        //try with HSB color space
//        if(rate == 0.0)
//        {
//            fromLoad = Color.getHSBColor(0f, 0f, 1f);
//        }
//        else
//        {
//            fromLoad = Color.getHSBColor((1 - rate) * 0.36f, 1f - (0.4f * (1 - rate)), 1f);
//        }
//        return fromLoad;
//    }
//    
//    public void fillNodesColor()
//    {
//        int load,x,y;
//        for(int i = 0; i < ds.schedulers.length; i++)
//        {
//            x = i % size;
//            y = i / size;
//            load = ds.schedulers[i].readyTaskListSize - ds.schedulers[i].numIdleCore;
//            this.nodesColor[y][x] = load2Color(load, x, y);
//        }
//    }
//
//    @Override
//    public Dimension getMaximumSize()
//    {
//        return new Dimension(windowSize,windowSize);
//    }
//
//    public Dimension getMinumumSize()
//    {
//        return new Dimension(windowSize,windowSize);
//    }
//
//    @Override
//    public Dimension getPreferredSize()
//    {
//        return new Dimension(windowSize,windowSize);
//    }
//   
//    @Override
//    public void paintComponent(Graphics g)
//    {
//        fillNodesColor();
//        for (int x = 0; x < size; x++)
//        {
//            for (int y = 0; y < size; y++)
//            {
//                g.setColor(nodesColor[x][y]);
//                g.fillRect(pixSize * y, pixSize * x, pixSize, pixSize);
//            }
//        }
//    }   
//}