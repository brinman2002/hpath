package org.apache.commons.jxpath.ri;

import org.apache.commons.jxpath.ri.compiler.Expression;

/**
 * ExpressionHelper is a class that is a part of the HPath project, that lives
 * in the jxpath package in order to easily consume a 'protected' method.
 *
 * @author brandon
 *
 */
public class ExpressionHelper {

    /**
     * Expose the {@link JXPathCompiledExpression#getExpression()} method.
     *
     * <p>
     * The method being exposed was given <code>protected</code> visibility by
     * its authors. Intentionally or otherwise, this marks the method as a
     * public API available to consumers. While the intention may have been to
     * make the method available to subclassers of
     * {@link JXPathCompiledExpression}, the method is subject to the same
     * limitations in terms of backwards compatibility under
     * <code>protected</code> scope as it would be under <code>public</code>
     * scope, and nothing stops a subclass from upgrading the method scope from
     * a less visible one to a more visible one.
     * </p>
     * <p>
     * Consuming this method in this manner allows us to avoid needlessly
     * duplicating the JXPath code into this project simply because of the
     * method scope.
     * </p>
     *
     * @param compiledExpression
     *            The compiled {@link JXPathCompiledExpression} from an XPath
     *            query.
     *
     * @return The {@link Expression} that underlies the
     *         {@link JXPathCompiledExpression}.
     */
    public static Expression getExpression(final JXPathCompiledExpression compiledExpression) {
        return compiledExpression.getExpression();
    }
}
