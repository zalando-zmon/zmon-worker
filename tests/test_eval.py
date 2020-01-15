import pytest
import ast
from zmon_worker_monitor.zmon_worker.common.eval import check_ast_node_is_safe, InvalidEvalExpression


def test_check_ast_node_is_safe_works():
    node = ast.parse('def __call__(): return 1')
    assert node == check_ast_node_is_safe(node, '<source>')


def test_check_ast_node_is_safe_fails_for_double_underscores():
    with pytest.raises(InvalidEvalExpression):
        check_ast_node_is_safe(ast.parse('instance._Instance__request'), '<source>')


def test_check_ast_node_is_safe_fails_for_double_underscores_method_calls():
    with pytest.raises(InvalidEvalExpression):
        check_ast_node_is_safe(ast.parse('def m(): return ().__class__'), '<hidden>')


def test_check_ast_node_is_safe_fails_for_exec():
    with pytest.raises(InvalidEvalExpression):
        check_ast_node_is_safe(ast.parse('def horror(g): exec "exploit = ().__" + "class" + "__" in g'), '<horror>')


def test_check_ast_node_is_safe_fails_for_exit():
    with pytest.raises(InvalidEvalExpression):
        check_ast_node_is_safe(ast.parse('def __exit__(): return 1'), '<hidden>')
