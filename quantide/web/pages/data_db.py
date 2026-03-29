"""数据库管理页面"""

import math
from fasthtml.common import *
import fasthtml.common as fh
from monsterui.all import *
from quantide.data.sqlite import db
from quantide.web.layouts.main import MainLayout
from quantide.web.theme import AppTheme, PRIMARY_COLOR
from loguru import logger

# 定义子路由应用
data_db_app, rt = fast_app(hdrs=AppTheme.headers())

def _get_active_tab(req):
    return req.query_params.get("tab", "data")

def _TabNav(active_tab: str):
    tabs = [
        ("data", "数据视图"),
        ("schema", "元数据视图"),
    ]
    
    tab_items = []
    for tab_id, label in tabs:
        is_active = active_tab == tab_id
        base_cls = "px-4 py-2 font-medium transition-colors duration-200"
        if is_active:
            cls = f"{base_cls} text-red-600 border-b-2 border-red-600"
        else:
            cls = f"{base_cls} text-gray-500 hover:text-gray-700 hover:border-b-2 hover:border-gray-300"
        
        tab_items.append(A(label, href=f"/data/db?tab={tab_id}", cls=cls))
        
    return Div(
        Div(*tab_items, cls="flex space-x-2"),
        cls="border-b border-gray-200 mb-6"
    )

def _DataTab(req):
    """数据视图 Tab 内容"""
    tables = db.db.table_names()
    
    # 获取选中的表
    selected_table = req.query_params.get("table", tables[0] if tables else "")
    
    # 获取分页参数
    page = int(req.query_params.get("page", 1))
    per_page = 20
    
    table_content = P("请选择要查看的表...", cls="text-gray-400 text-center py-12")
    
    if selected_table and selected_table in tables:
        try:
            total_count = db.db[selected_table].count
            total_pages = math.ceil(total_count / per_page)
            
            # 修正页码范围
            page = max(1, min(page, total_pages)) if total_pages > 0 else 1
            offset = (page - 1) * per_page
            
            # 获取表头
            columns = [col.name for col in db.db[selected_table].columns]
            headers = ["选择"] + columns
            header_row = Tr(*[Th(h) for h in headers])
            
            # 获取数据行
            rows = list(db.db[selected_table].rows_where(limit=per_page, offset=offset))
            
            if rows:
                table_rows = []
                for row in rows:
                    cells = [Td(Input(type="checkbox", name="selected_ids", value=row.get("id", ""), cls="checkbox checkbox-sm"))]
                    for col in columns:
                        cells.append(Td(str(row.get(col, ""))))
                    table_rows.append(Tr(*cells, cls="hover:bg-gray-50"))
                    
                table_content = Div(
                    Table(Thead(header_row), Tbody(*table_rows), cls="uk-table uk-table-divider uk-table-small text-sm"),
                    _build_pagination(selected_table, page, total_pages)
                )
            else:
                table_content = P("表中无数据", cls="text-gray-500 text-center py-12")
                
        except Exception as e:
            logger.error(f"加载表数据失败: {e}")
            table_content = P(f"加载数据出错: {e}", cls="text-red-500 text-center py-12")

    return Div(
        Form(
            Div(
                fh.Select(
                    *[fh.Option(t, value=t, selected=t == selected_table) for t in tables],
                    name="table",
                    cls="uk-select w-64",
                    onchange="this.form.submit()"
                ),
                Button(UkIcon("trash-2", cls="mr-1"), "删除选中行", type="button", cls="btn btn-ghost text-red-600"),
                Button(UkIcon("save", cls="mr-1"), "保存修改", type="button", cls="btn btn-primary ml-auto"),
                cls="flex items-center gap-4 mb-4"
            ),
            method="GET",
            action="/data/db"
        ),
        Input(type="hidden", name="tab", value="data"),
        Div(table_content, cls="overflow-x-auto bg-white border rounded-lg"),
        cls="animate-in fade-in duration-500"
    )

def _build_pagination(table: str, current_page: int, total_pages: int):
    """构建分页控件"""
    if total_pages <= 1:
        return Div()
        
    items = []
    
    # 上一页
    if current_page > 1:
        items.append(A("上一页", href=f"/data/db?tab=data&table={table}&page={current_page-1}", cls="btn btn-sm btn-ghost"))
        
    # 页码指示
    items.append(Span(f"第 {current_page} 页 / 共 {total_pages} 页", cls="px-4 py-1 text-sm text-gray-600"))
    
    # 下一页
    if current_page < total_pages:
        items.append(A("下一页", href=f"/data/db?tab=data&table={table}&page={current_page+1}", cls="btn btn-sm btn-ghost"))
        
    return Div(*items, cls="flex items-center justify-center p-4 border-t")

def _SchemaTab(req):
    """元数据视图 Tab 内容"""
    tables = db.db.table_names()
    
    cards = []
    for table_name in tables:
        columns = db.db[table_name].columns
        rows = []
        for col in columns:
            rows.append(Tr(
                Td(col.name, cls="font-mono text-blue-600 font-medium"),
                Td(col.type, cls="text-gray-500"),
                Td("是" if col.is_pk else "否", cls="text-gray-400"),
            ))
            
        cards.append(
            Card(
                CardHeader(H4(table_name, cls="font-bold text-lg")),
                CardBody(
                    Table(
                        Thead(Tr(Th("列名"), Th("类型"), Th("主键"))),
                        Tbody(*rows),
                        cls="uk-table uk-table-small text-sm"
                    )
                ),
                cls="mb-6 shadow-sm"
            )
        )
        
    return Div(
        *cards,
        cls="animate-in fade-in duration-500"
    )

@rt("/")
async def index(req):
    active_tab = _get_active_tab(req)
    
    if active_tab == "schema":
        content = _SchemaTab(req)
    else:
        content = _DataTab(req)
        
    layout = MainLayout()
    layout.set_sidebar_active("/data/db")
    
    page_content = Div(
        Div(
            Div(
                UkIcon("database", size=32, cls="mr-3", style=f"color: {PRIMARY_COLOR};"),
                H2("数据库浏览器", cls="text-2xl font-bold"),
                cls="flex items-center"
            ),
            cls="mb-8"
        ),
        _TabNav(active_tab),
        content,
        cls="p-8"
    )
    
    layout.main_block = page_content
    return layout.render()
