""" 读取流向数据,从源头开始按流向寻找其汇入哪条河流,汇入同一河流的算作一个流域,废弃 """
from osgeo import gdal,ogr
import numpy as np

def traverse_band1_pixels(raster_path):
    """
    遍历栅格数据波段1的所有像素值
    
    参数:
        raster_path (str): 栅格文件路径
        
    返回:
        None
    """
    # 注册所有GDAL驱动
    gdal.AllRegister()
    # 设置Shapefile UTF-8编码
    gdal.SetConfigOption("SHAPE_ENCODING", "UTF-8")
    
    # 打开栅格数据集
    dataset = gdal.Open(raster_path, gdal.GA_ReadOnly)
    if dataset is None:
        print("无法打开栅格文件")
        return
        
    # 获取波段1
    band = dataset.GetRasterBand(1)
    print("波段1的数据类型:", gdal.GetDataTypeName(band.DataType))
    
    # 读取整个波段数据为numpy数组
    data = band.ReadAsArray()
    
    # 获取无数据值，0
    no_data = band.GetNoDataValue()
    print("读取数据完成")
    # 遍历所有像素
    rows, cols = data.shape
    print("像素个数",rows*cols)
    # for row in range(rows):
    #     for col in range(cols):
    #         i=i+1
    #         pixel_value = data[row, col]
                
    #         # 在这里处理每个像素值
    #         # 示例: 打印前10个像素值
    #         if pixel_value!=0:
    #             print(f"像素位置({row}, {col}) 值: {pixel_value}")
    
    # 关闭数据集
    dataset = None
    print("\n波段1像素遍历完成")

def get_pixel_center_coordinates(row,col,gt):
    '''
    # 获取像素坐标
    获取地理变换参数
    gt = dataset.GetGeoTransform()
    地理变换参数含义:
    gt[0] = 左上角X坐标
    gt[1] = 东西方向像素分辨率
    gt[2] = 旋转参数(通常为0)
    gt[3] = 左上角Y坐标
    gt[4] = 旋转参数(通常为0)
    gt[5] = 南北方向像素分辨率(通常为负值)
    '''
    x_coord = gt[0] + (col + 0.5) * gt[1] + (row + 0.5) * gt[2]
    y_coord = gt[3] + (col + 0.5) * gt[4] + (row + 0.5) * gt[5]

    return x_coord,y_coord

def is_point_in_polygon(point_x, point_y, spatial_index):
    """
    # 判断单个坐标点是否在面要素内
    
    参数:
        point_x (float): 点的X坐标
        point_y (float): 点的Y坐标
        spatial_index (ogr.Layer): 空间索引

    # 创建河道面空间索引
    polygon_file=""
    driver = ogr.GetDriverByName('ESRI Shapefile')
    datasource = driver.Open(polygon_file, 0)
    layer = datasource.GetLayer()
    layer.ResetReading()
    spatial_index = ogr.CreateSpatialIndex()
    for feature in layer:
        geom = feature.GetGeometryRef()
        spatial_index.Insert(feature.GetFID(), geom.GetEnvelope())
    返回:
        bool: 如果点在面内返回True,否则返回False
    """
    # 创建点几何对象
    point = ogr.Geometry(ogr.wkbPoint)
    point.AddPoint(point_x, point_y)
            
    # 使用空间索引快速筛选可能包含该点的面
    candidates = spatial_index.Intersects(point.GetEnvelope())
    inside = False
    for fid in candidates:
        feature = layer.GetFeature(fid)
        geom = feature.GetGeometryRef()
        if geom.Contains(point):
            inside = True
            break
    return inside

# 使用示例
if __name__ == "__main__":
    # 流向路径
    lx_path = "F:\实习\超图(钱管局)实习\流域\测试\cslx1.tif"
    # 流量路径
    ll_path = "F:\实习\超图(钱管局)实习\流域\测试\csll1.tif"
    # 结果路径

    # 注册所有GDAL驱动
    gdal.AllRegister()
    # 设置Shapefile UTF-8编码
    gdal.SetConfigOption("SHAPE_ENCODING", "UTF-8")

    # 打开栅格数据集,并获取第一波段
    lx = gdal.Open(lx_path, gdal.GA_ReadOnly)
    if lx is None:
        print(f"无法打开{lx_path}")
    lx_band = lx.GetRasterBand(1)

    # 打开栅格数据集
    ll = gdal.Open(ll_path, gdal.GA_ReadOnly)
    if ll is None:
        print(f"无法打开{ll_path}")
    ll_band = ll.GetRasterBand(1)


    # 读取流向、流量数据为numpy数组
    lx_data = lx_band.ReadAsArray()
    ll_data = ll_band.ReadAsArray()

    # 流量流向gt完全一样
    gt = lx.GetGeoTransform()

    # 读取河流矢量面数据
    polygon_file="F:\实习\超图(钱管局)实习\流域\测试\测试河道.shp"
    driver = ogr.GetDriverByName('ESRI Shapefile')
    datasource = driver.Open(polygon_file, 0)
    layer = datasource.GetLayer()
    # 创建河道面空间索引
    layer.ResetReading()
    spatial_index = ogr.CreateSpatialIndex()
    for feature in layer:
        geom = feature.GetGeometryRef()
        spatial_index.Insert(feature.GetFID(), geom.GetEnvelope())

    # 遍历流量栅格，读取其中值为0的栅格作为起点
    rows, cols = ll_data.shape
    for row in range(rows):
        for col in range(cols):
            if ll_data[row, col]!=0:
                isFindRiver=False
                while not isFindRiver:
                    pixel_value = lx_data[row, col]
                    x_coord,y_coord=get_pixel_center_coordinates(row, col,gt)
                    print("坐标",x_coord,y_coord)
                    print("是否在面内",is_point_in_polygon(x_coord,y_coord,spatial_index))
                    break
                break
        break